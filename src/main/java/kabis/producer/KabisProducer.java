package kabis.producer;

import kabis.storage.MessageWrapper;
import kabis.validation.KabisServiceProxy;
import kabis.validation.SecureIdentifier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class KabisProducer<K extends Integer, V extends String> implements KabisProducerI<K, V> {

    private final Set<String> validatedTopics = new HashSet<>();
    private final List<KafkaProducer<K, MessageWrapper<V>>> kafkaProducers;
    private final KabisServiceProxy serviceProxy;
    private final int id;

    public KabisProducer(Properties properties) {
        var bootstrapServers = properties.getProperty("bootstrap.servers").split(";");
        this.id = Integer.parseInt(properties.getProperty("client.id"));
        this.kafkaProducers = new ArrayList<>(bootstrapServers.length);
        for (int i = 0; i < bootstrapServers.length; i++) {
            String server = bootstrapServers[i];
            var id = String.format("%d-producer-%d", this.id, i);
            var simplerProperties = (Properties) properties.clone();
            simplerProperties.put("bootstrap.servers", server);
            simplerProperties.put("client.id", id);
            this.kafkaProducers.add(new KafkaProducer<>(simplerProperties));
        }
        this.serviceProxy = new KabisServiceProxy(id);
    }

    @Override
    public void updateTopology(Collection<String> validatedTopics) {
        synchronized (this.validatedTopics) {
            this.validatedTopics.clear();
            this.validatedTopics.addAll(validatedTopics);
        }
    }

    @Override
    public void push(ProducerRecord<K, V> record) {
        synchronized (validatedTopics) {
            if (validatedTopics.contains(record.topic())) {
                pushValidated(record);
            } else {
                pushUnvalidated(record);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void pushValidated(ProducerRecord<K, V> record) {
        var value = record.value();
        var key = record.key();
        var wrapper = new MessageWrapper<>(value, id);
        var wrappedRecord = new ProducerRecord<>(record.topic(), record.key(), wrapper);


        CompletableFuture<RecordMetadata>[] completableFutures = kafkaProducers.stream().map(prod -> {
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            prod.send(wrappedRecord, ((metadata, exception) -> {
                if (exception == null) future.complete(metadata);
                else future.completeExceptionally(exception);
            }));
            return future;
        }).toArray(CompletableFuture[]::new);

        CompletableFuture.anyOf(completableFutures)
                .thenAccept(res -> {
                    RecordMetadata metadata = (RecordMetadata) res;
                    var topic = metadata.topic();
                    var partition = metadata.partition();
                    SecureIdentifier sid = SecureIdentifier.factory(key, value, topic, partition, id);
                    serviceProxy.push(sid);
                }).join();
        CompletableFuture.allOf(completableFutures).join();
    }


    private void pushUnvalidated(ProducerRecord<K, V> record) {
        var value = record.value();
        var wrapper = new MessageWrapper<>(value);
        var wrappedRecord = new ProducerRecord<>(record.topic(), record.key(), wrapper);
        kafkaProducers.get(0).send(wrappedRecord);
    }

    @Override
    public void flush() {
        kafkaProducers.forEach(KafkaProducer::flush);
    }

    @Override
    public void close() {
        kafkaProducers.forEach(KafkaProducer::close);

    }

    @Override
    public void close(Duration duration) {
        kafkaProducers.forEach(p -> p.close(duration));
    }
}
