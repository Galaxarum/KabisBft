package kabis.consumer;

import kabis.storage.MessageWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaPollingThread<K, V> {
    /**
     * List of Kafka consumers, one for each Kafka replica.
     */
    private final List<KafkaConsumer<K, MessageWrapper<V>>> consumers;
    /**
     * List of caches, one for each Kafka replica.
     */
    private final List<Cache<K, V>> cacheReplicas;

    /**
     * Creates a new KafkaPollingThread.
     *
     * @param properties the properties to be used by the KafkaPollingThread
     */
    public KafkaPollingThread(Properties properties) {
        //TODO: Check if the properties are valid, otherwise throw an exception
        String[] serversReplicas = properties.getProperty("bootstrap.servers").split(";");
        ArrayList<KafkaConsumer<K, MessageWrapper<V>>> consumers = new ArrayList<>(serversReplicas.length);
        this.cacheReplicas = new ArrayList<>(serversReplicas.length);
        for (int i = 0; i < serversReplicas.length; i++) {
            String servers = serversReplicas[i];
            String id = String.format("%s-consumer-%d", properties.getProperty("client.id"), i);
            Properties simplerProperties = (Properties) properties.clone();
            simplerProperties.put("bootstrap.servers", servers);
            simplerProperties.put("client.id", id);
            consumers.add(new KafkaConsumer<>(simplerProperties));
            this.cacheReplicas.add(new Cache<>());
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
        pullKafka(0, Duration.ofMinutes(1));
        Set<TopicPartition> assignedPartitions = this.consumers.get(0).assignment();
        System.out.println("[getAssignedPartitions] Replica 0: " + assignedPartitions);
        for (int replicaIndex = 1; replicaIndex < this.cacheReplicas.size(); replicaIndex++) {
            pullKafka(replicaIndex, Duration.ofMinutes(1));
            Set<TopicPartition> assignedPartitionsReplica = this.consumers.get(replicaIndex).assignment();
            System.out.println("[getAssignedPartitions] Replica 1: " + assignedPartitions);
            if (!assignedPartitionsReplica.equals(assignedPartitions)) {
                throw new IllegalStateException("The Kafka replicas have different assigned partitions");
            }
        }
        return new ArrayList<>(assignedPartitions);
    }

    /**
     * Polls for records and stores them in the cache.
     *
     * @param timeout The maximum time to block (must not be greater than Long.MAX_VALUE milliseconds)
     */
    private void pullKafka(int replicaIndex, Duration timeout) {
        ConsumerRecords<K, MessageWrapper<V>> records = this.consumers.get(replicaIndex).poll(timeout);
        Cache<K, V> cache = this.cacheReplicas.get(replicaIndex);
        for (ConsumerRecord<K, MessageWrapper<V>> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            CacheKey key = new CacheKey(tp, record.value().getSenderId());
            cache.offer(key, record);
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
                pullKafka(replicaIndex, timeout);
            }
            res.add(cache.poll(cacheKey));
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
        pullKafka(0, timeout);
        Cache<K, V> cache = this.cacheReplicas.get(0);

        Set<CacheKey> validTPs = cache.getKeys();
        validTPs.removeIf(k -> excludedTopics.contains(k.topicPartition.topic()));
        if (validTPs.isEmpty()) return Map.of();

        HashMap<TopicPartition, List<ConsumerRecord<K, V>>> map = new HashMap<>();
        for (CacheKey tp : validTPs) {
            List<ConsumerRecord<K, MessageWrapper<V>>> queue = cache.drain(tp);
            List<ConsumerRecord<K, V>> list = map.computeIfAbsent(tp.topicPartition, ignored -> new LinkedList<>());
            //TODO: new ConsumerRecord<> is not adding the headers and timestamp
            list.addAll(queue.stream()
                    .map(cr -> new ConsumerRecord<>(cr.topic(), cr.partition(), cr.offset(), cr.key(), cr.value().getValue()))
                    .collect(Collectors.toList()));
        }
        return map;
    }

    public void subscribe(Collection<String> topics) {
        this.consumers.forEach(c -> c.subscribe(topics));
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
