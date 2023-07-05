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
    private final List<KafkaConsumer<K, MessageWrapper<V>>> consumers;
    private final List<Cache<K, V>> caches;

    /**
     * Creates a new KafkaPollingThread.
     *
     * @param properties the properties to be used by the KafkaPollingThread
     */
    public KafkaPollingThread(Properties properties) {
        String[] bootstrapServers = properties.getProperty("bootstrap.servers").split(";");
        ArrayList<KafkaConsumer<K, MessageWrapper<V>>> consumers = new ArrayList<>(bootstrapServers.length);
        this.caches = new ArrayList<>(bootstrapServers.length);
        for (int i = 0; i < bootstrapServers.length; i++) {
            String servers = bootstrapServers[i];
            String id = String.format("%s-consumer-%d", properties.getProperty("client.id"), i);
            Properties simplerProperties = (Properties) properties.clone();
            simplerProperties.put("bootstrap.servers", servers);
            simplerProperties.put("client.id", id);
            consumers.add(new KafkaConsumer<>(simplerProperties));
            caches.add(new Cache<>());
        }
        this.consumers = Collections.unmodifiableList(consumers);
    }

    public synchronized List<ConsumerRecord<K, MessageWrapper<V>>> poll(TopicPartition tp, int producerId, Duration timeout) {
        CacheKey cacheKey = new CacheKey(tp, producerId);
        ArrayList<ConsumerRecord<K, MessageWrapper<V>>> res = new ArrayList<>(caches.size());
        for (int replicaIndex = 0; replicaIndex < caches.size(); replicaIndex++) {
            Cache<K, V> cache = caches.get(replicaIndex);
            while (!cache.hasAny(cacheKey)) {
                pullKafka(replicaIndex, timeout);
            }
            res.add(cache.poll(cacheKey));
        }
        return res;
    }

    private void pullKafka(int replicaIndex, Duration timeout) {
        ConsumerRecords<K, MessageWrapper<V>> records = consumers.get(replicaIndex).poll(timeout);
        Cache<K, V> cache = caches.get(replicaIndex);
        for (var record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            CacheKey key = new CacheKey(tp, record.value().getSenderId());
            cache.offer(key, record);
        }
    }

    public synchronized Map<TopicPartition, List<ConsumerRecord<K, V>>> pollUnvalidated(Collection<String> excludedTopics, Duration timeout) {
        pullKafka(0, timeout);
        Cache<K, V> cache = caches.get(0);

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
        consumers.forEach(c -> c.subscribe(topics));
    }

    public void unsubscribe() {
        consumers.forEach(KafkaConsumer::unsubscribe);
    }

    public void close() {
        consumers.forEach(KafkaConsumer::close);
    }

    public void close(Duration duration) {
        consumers.forEach(c -> c.close(duration));
    }
}

class Cache<K, V> {
    private final Map<CacheKey, Queue<ConsumerRecord<K, MessageWrapper<V>>>> data = new HashMap<>();

    public synchronized void offer(CacheKey key, ConsumerRecord<K, MessageWrapper<V>> value) {
        if (!data.containsKey(key)) data.put(key, new LinkedList<>());
        Queue<ConsumerRecord<K, MessageWrapper<V>>> queue = data.get(key);
        queue.offer(value);
    }

    public synchronized boolean hasAny(CacheKey key) {
        return data.containsKey(key) && !data.get(key).isEmpty();
    }

    public synchronized ConsumerRecord<K, MessageWrapper<V>> poll(CacheKey key) {
        return data.get(key).poll();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        data.entrySet().stream()
                .sorted((e1, e2) -> CacheKey.compare(e1.getKey(), e2.getKey()))
                .map(e -> Map.entry(e.getKey(), e.getValue().stream().map(ConsumerRecord::key).collect(Collectors.toList())))
                .forEach(sb::append);
        return sb.toString();
    }

    public synchronized Set<CacheKey> getKeys() {
        return new HashSet<>(data.keySet());
    }

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

