package kabis.consumer;

import kabis.storage.MessageWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaPollingThread <K,V>{
    private final List<KafkaConsumer<K,MessageWrapper<V>>> consumers;
    private final List<Cache<K,V>> caches;

    public KafkaPollingThread(Properties properties){
        var bootstrapServers = properties.getProperty("bootstrap.servers").split(";");
        var consumers = new ArrayList<KafkaConsumer<K,MessageWrapper<V>>>(bootstrapServers.length);
        this.caches = new ArrayList<>(bootstrapServers.length);
        for (int i = 0; i < bootstrapServers.length; i++) {
            String server = bootstrapServers[i];
            var id = String.format("%s-consumer-%d", properties.getProperty("client.id"), i);
            var simplerProperties = (Properties) properties.clone();
            simplerProperties.put("bootstrap.servers", server);
            simplerProperties.put("client.id",id);
            consumers.add(new KafkaConsumer<>(simplerProperties));
            caches.add(new Cache<>());
        }
        this.consumers = Collections.unmodifiableList(consumers);
    }

    public synchronized List<ConsumerRecord<K,MessageWrapper<V>>> poll(TopicPartition tp, int producerId,Duration timeout){
        var cacheKey = new CacheKey(tp, producerId);
        var res = new ArrayList<ConsumerRecord<K,MessageWrapper<V>>>(caches.size());
        for(int replicaIndex = 0; replicaIndex<caches.size();replicaIndex++){
            var cache = caches.get(replicaIndex);
            while (!cache.hasAny(cacheKey)){
                pullKafka(replicaIndex,timeout);
            }
            res.add(cache.poll(cacheKey));
        }
        return res;
    }

    public synchronized Map<TopicPartition,List<ConsumerRecord<K,V>>> pollUnvalidated(Collection<String> excludedTopics,Duration timeout){
        pullKafka(0,timeout);
        var cache = caches.get(0);

        var validTPs = cache.getKeys();
        validTPs.removeIf(k->excludedTopics.contains(k.topicPartition.topic()));
        if(validTPs.isEmpty()) return Map.of();

        var map = new HashMap<TopicPartition,List<ConsumerRecord<K,V>>>();
        for(var tp: validTPs){
            var queue = cache.drain(tp);
            var list = map.computeIfAbsent(tp.topicPartition,ignored->new LinkedList<>());
            list.addAll(queue.stream()
                    .map(cr->new ConsumerRecord<>(cr.topic(),cr.partition(),cr.offset(),cr.key(),cr.value().getValue()))
                    .collect(Collectors.toList()));
        }
        return map;

    }

    private void pullKafka(int replicaIndex,Duration timeout){
        var records = consumers.get(replicaIndex).poll(timeout);
        var cache = caches.get(replicaIndex);
        for(var record: records){
            var tp = new TopicPartition(record.topic(),record.partition());
            var key = new CacheKey(tp,record.value().getSenderId());
            cache.offer(key,record);
        }
    }

    public void subscribe(Collection<String> topics){
        consumers.forEach(c->c.subscribe(topics));
    }

    public void unsubscribe(){
        consumers.forEach(KafkaConsumer::unsubscribe);
    }

    public void close() {
        consumers.forEach(KafkaConsumer::close);
    }

    public void close(Duration duration) {
        consumers.forEach(c->c.close(duration));
    }
}

class Cache<K,V>{
    private final Map<CacheKey,Queue<ConsumerRecord<K,MessageWrapper<V>>>> data = new HashMap<>();

    public synchronized void offer(CacheKey key,ConsumerRecord<K,MessageWrapper<V>> value){
        if(!data.containsKey(key)) data.put(key,new LinkedList<>());
        var queue = data.get(key);
        queue.offer(value);
    }

    public synchronized boolean hasAny(CacheKey key){
        return data.containsKey(key) && !data.get(key).isEmpty();
    }

    public synchronized ConsumerRecord<K,MessageWrapper<V>> poll(CacheKey key){
        return data.get(key).poll();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        data.entrySet().stream()
                .sorted((e1,e2)->CacheKey.compare(e1.getKey(),e2.getKey()))
                .map(e->Map.entry(e.getKey(),e.getValue().stream().map(ConsumerRecord::key).collect(Collectors.toList())))
                .forEach(sb::append);
        return sb.toString();
    }

    public synchronized Set<CacheKey> getKeys() {
        return new HashSet<>(data.keySet());
    }

    public synchronized List<ConsumerRecord<K, MessageWrapper<V>>> drain(CacheKey key) {
        var queue = data.get(key);
        var res = new ArrayList<>(queue);
        queue.clear();
        return res;
    }
}

class CacheKey{
    final TopicPartition topicPartition;
    final int producerId;


    CacheKey(TopicPartition topicPartition, int producerId) {
        this.topicPartition = topicPartition;
        this.producerId = producerId;
    }

    @Override
    public String toString() {
        return "CacheKey{" +
                "topicPartition=" + topicPartition +
                ", producerId=" + producerId +
                '}';
    }

    public static int compare(CacheKey o1, CacheKey o2) {
        var compareProducer = Integer.compare(o1.producerId,o2.producerId);
        if(compareProducer!=0) return compareProducer;
        var compareTopic = o1.topicPartition.topic().compareTo(o2.topicPartition.topic());
        if(compareTopic!=0) return  compareTopic;
        return Integer.compare(o1.topicPartition.partition(),o2.topicPartition.partition());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheKey cacheKey = (CacheKey) o;
        return producerId == cacheKey.producerId && topicPartition.equals(cacheKey.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, producerId);
    }
}

