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

class Cache<K, V> {
    private final Map<CacheKey, Queue<ConsumerRecord<K, MessageWrapper<V>>>> data = new HashMap<>();

    /**
     * Adds the given value to the queue of the given key.
     *
     * @param key   the key to add the value to
     * @param value the value to add
     */
    public synchronized void offer(CacheKey key, ConsumerRecord<K, MessageWrapper<V>> value) {
        if (!data.containsKey(key)) data.put(key, new LinkedList<>());
        Queue<ConsumerRecord<K, MessageWrapper<V>>> queue = this.data.get(key);
        queue.offer(value);
    }

    /**
     * Returns true if there is at least one element in the queue of the given key.
     *
     * @param key the key to check
     * @return true if there is at least one element in the queue of the given key
     */
    public synchronized boolean hasAny(CacheKey key) {
        return this.data.containsKey(key) && !data.get(key).isEmpty();
    }

    /**
     * Returns the first element of the queue of the given key.
     *
     * @param key the key to check
     * @return the first element of the queue of the given key
     */
    public synchronized ConsumerRecord<K, MessageWrapper<V>> poll(CacheKey key) {
        return this.data.get(key).poll();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        this.data.entrySet().stream()
                .sorted((e1, e2) -> CacheKey.compare(e1.getKey(), e2.getKey()))
                .map(e -> Map.entry(e.getKey(), e.getValue().stream().map(ConsumerRecord::key).collect(Collectors.toList())))
                .forEach(sb::append);
        return sb.toString();
    }

    public synchronized Set<CacheKey> getKeys() {
        return new HashSet<>(this.data.keySet());
    }

    /**
     * Clears the queue of the given key and returns all the elements.
     *
     * @param key the key to check
     * @return all the elements of the queue of the given key
     */
    public synchronized List<ConsumerRecord<K, MessageWrapper<V>>> drain(CacheKey key) {
        Queue<ConsumerRecord<K, MessageWrapper<V>>> queue = data.get(key);
        ArrayList<ConsumerRecord<K, MessageWrapper<V>>> res = new ArrayList<>(queue);
        queue.clear();
        return res;
    }
}

class CacheKey {
    final TopicPartition topicPartition;
    final int producerId;


    CacheKey(TopicPartition topicPartition, int producerId) {
        this.topicPartition = topicPartition;
        this.producerId = producerId;
    }

    public static int compare(CacheKey o1, CacheKey o2) {
        int compareProducer = Integer.compare(o1.producerId, o2.producerId);
        if (compareProducer != 0) return compareProducer;
        int compareTopic = o1.topicPartition.topic().compareTo(o2.topicPartition.topic());
        if (compareTopic != 0) return compareTopic;
        return Integer.compare(o1.topicPartition.partition(), o2.topicPartition.partition());
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, producerId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheKey cacheKey = (CacheKey) o;
        return producerId == cacheKey.producerId && topicPartition.equals(cacheKey.topicPartition);
    }

    @Override
    public String toString() {
        return "CacheKey{" +
                "topicPartition=" + topicPartition +
                ", producerId=" + producerId +
                '}';
    }
}

