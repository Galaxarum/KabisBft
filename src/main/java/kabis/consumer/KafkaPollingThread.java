package kabis.consumer;

import kabis.storage.MessageWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaPollingThread<K extends Integer, V extends String> {
    /**
     * List of Kafka consumers, one for each Kafka replica.
     */
    private final List<KafkaConsumer<K, MessageWrapper<V>>> consumers;
    /**
     * List of caches, one for each Kafka replica.
     */
    private final List<Cache<K, V>> cacheReplicas;
    private final Logger log;

    private final Map<Integer, List<TopicPartition>> replicaAssignedPartitions = new HashMap<>();


    /**
     * Creates a new KafkaPollingThread.
     *
     * @param properties the properties to be used by the KafkaPollingThread
     */
    public KafkaPollingThread(Properties properties) {
        this.log = LoggerFactory.getLogger(KafkaPollingThread.class);
        String[] serversReplicas = properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).split(";");
        ArrayList<KafkaConsumer<K, MessageWrapper<V>>> consumers = new ArrayList<>(serversReplicas.length);
        this.cacheReplicas = new ArrayList<>(serversReplicas.length);
        for (int i = 0; i < serversReplicas.length; i++) {
            String servers = serversReplicas[i];
            String id = String.format("%s-consumer-%d", properties.getProperty("client.id"), i);
            Properties simplerProperties = (Properties) properties.clone();
            simplerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            simplerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
            consumers.add(new KafkaConsumer<>(simplerProperties));
            this.cacheReplicas.add(new Cache<>());
            this.replicaAssignedPartitions.put(i, new ArrayList<>());
        }
        this.consumers = Collections.unmodifiableList(consumers);
    }

    /**
     * Returns the list of assigned partitions to the Kafka consumers.
     *
     * @return the list of assigned partitions to the Kafka consumers
     * @throws IllegalStateException if the Kafka replicas have different assigned partitions
     */
    public List<TopicPartition> getAssignedPartitions() {
        Set<TopicPartition> firstReplicaAssignedPartitions = pullAndReturnAssignedPartitions(0);
        this.log.info("Replica 0, assigned partitions: {}", firstReplicaAssignedPartitions);

        for (int replicaIndex = 1; replicaIndex < this.consumers.size(); replicaIndex++) {
            Set<TopicPartition> assignedPartitionsReplica = pullAndReturnAssignedPartitions(replicaIndex);
            this.log.info("Replica {}, assigned partitions: {}", replicaIndex, assignedPartitionsReplica);
            if (!assignedPartitionsReplica.equals(firstReplicaAssignedPartitions)) {
                throw new IllegalStateException("The Kafka replicas have different assigned partitions");
            }
        }
        return new ArrayList<>(firstReplicaAssignedPartitions);
    }

    /**
     * Returns the list of assigned partitions to the Kafka consumer of the given replica.
     *
     * @return the set of assigned partitions to the Kafka consumers
     */
    private Set<TopicPartition> pullAndReturnAssignedPartitions(int replicaIndex) {
        pullKafka(replicaIndex, Duration.ofSeconds(30), false);
        Set<TopicPartition> assignedPartitions = this.consumers.get(replicaIndex).assignment();
        while (assignedPartitions.isEmpty()) {
            pullKafka(replicaIndex, Duration.ofSeconds(30), false);
            assignedPartitions = this.consumers.get(replicaIndex).assignment();
        }
        List<TopicPartition> assignedPartitionsList = this.replicaAssignedPartitions.get(replicaIndex);
        assignedPartitions.forEach(tp -> {
            if (!assignedPartitionsList.contains(tp)) {
                this.log.info("Replica {}, new assigned partition: {}, seeking to offset 0", replicaIndex, tp);
                this.consumers.get(replicaIndex).seek(tp, 0);
                assignedPartitionsList.add(tp);
            }
        });
        return assignedPartitions;
    }

    /**
     * Polls for records and stores them in the cache.
     *
     * @param timeout The maximum time to block (must not be greater than Long.MAX_VALUE milliseconds)
     */
    private void pullKafka(int replicaIndex, Duration timeout, boolean updateCache) {
        ConsumerRecords<K, MessageWrapper<V>> records = this.consumers.get(replicaIndex).poll(timeout);
        if (updateCache) {
            Cache<K, V> cache = this.cacheReplicas.get(replicaIndex);
            for (ConsumerRecord<K, MessageWrapper<V>> record : records) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                CacheKey key = new CacheKey(tp, record.value().getSenderId());
                cache.offer(key, record);
            }
        }
    }

    /**
     * Polls for records from each Kafka replica, and returns a list of records from each replica.
     *
     * @param tp         The topic partition to poll from
     * @param producerId The producer id to poll from
     * @param timeout    The maximum time to block (must not be greater than Long.MAX_VALUE milliseconds)
     * @return the records from each replica
     */
    public synchronized List<ConsumerRecord<K, MessageWrapper<V>>> poll(TopicPartition tp, int producerId, Duration timeout) {
        CacheKey cacheKey = new CacheKey(tp, producerId);
        ArrayList<ConsumerRecord<K, MessageWrapper<V>>> res = new ArrayList<>(this.cacheReplicas.size());
        for (int replicaIndex = 0; replicaIndex < this.cacheReplicas.size(); replicaIndex++) {
            Cache<K, V> cache = this.cacheReplicas.get(replicaIndex);
            while (!cache.hasAny(cacheKey)) {
                pullKafka(replicaIndex, timeout, true);
            }
            if (cache.hasAny(cacheKey)) res.add(cache.poll(cacheKey));
        }
        return res;
    }


    /**
     * Polls for records from each Kafka replica, and returns a list of records from each replica.
     *
     * @param excludedTopics The validated topics that must be excluded from the unvalidated polling
     * @param timeout        The maximum time to block (must not be greater than Long.MAX_VALUE milliseconds)
     * @return the consumer records grouped by partition
     */
    public synchronized Map<TopicPartition, List<ConsumerRecord<K, V>>> pollUnvalidated(Collection<String> excludedTopics, Duration timeout) {
        pullKafka(0, timeout, true);
        Cache<K, V> cache = this.cacheReplicas.get(0);

        Set<CacheKey> validTPs = cache.getKeys();
        validTPs.removeIf(k -> excludedTopics.contains(k.topicPartition.topic()));
        if (validTPs.isEmpty()) return Map.of();

        HashMap<TopicPartition, List<ConsumerRecord<K, V>>> map = new HashMap<>();
        for (CacheKey tp : validTPs) {
            List<ConsumerRecord<K, MessageWrapper<V>>> queue = cache.drain(tp);
            List<ConsumerRecord<K, V>> list = map.computeIfAbsent(tp.topicPartition, ignored -> new LinkedList<>());
            list.addAll(queue.stream()
                    .map(cr -> new ConsumerRecord<>(cr.topic(), cr.partition(), cr.offset(), cr.timestamp(), cr.timestampType(), cr.serializedKeySize(),
                            cr.serializedValueSize(), cr.key(), cr.value().getValue(), cr.headers(), cr.leaderEpoch()))
                    .collect(Collectors.toList()));
        }
        return map;
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * Topic subscriptions are not incremental. This list will replace the current assignment (if there is one).
     *
     * @param topics The topics to subscribe to
     */
    public void subscribe(Collection<String> topics) {
        this.consumers.forEach(consumer -> consumer.subscribe(topics));
    }

    public void unsubscribe() {
        this.consumers.forEach(KafkaConsumer::unsubscribe);
    }

    public void close() {
        this.consumers.forEach(KafkaConsumer::close);
    }

    public void close(Duration duration) {
        this.consumers.forEach(c -> c.close(duration));
    }
}
