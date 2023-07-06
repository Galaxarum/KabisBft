package kabis.consumer;

import kabis.storage.MessageWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;

public class Cache<K, V> {
    private final Map<CacheKey, Queue<ConsumerRecord<K, MessageWrapper<V>>>> data = new HashMap<>();

    /**
     * Adds the given value to the queue of the given key.
     *
     * @param key   the key to add the value to
     * @param value the value to add
     */
    public synchronized void offer(CacheKey key, ConsumerRecord<K, MessageWrapper<V>> value) {
        if (!this.data.containsKey(key)) this.data.put(key, new LinkedList<>());
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
        return this.data.containsKey(key) && !this.data.get(key).isEmpty();
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
        Queue<ConsumerRecord<K, MessageWrapper<V>>> queue = this.data.get(key);
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
        return Objects.hash(this.topicPartition, this.producerId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheKey cacheKey = (CacheKey) o;
        return this.producerId == cacheKey.producerId && this.topicPartition.equals(cacheKey.topicPartition);
    }

    @Override
    public String toString() {
        return "CacheKey{" + "getTopicPartition=" + this.topicPartition + ", producerId=" + this.producerId + '}';
    }
}
