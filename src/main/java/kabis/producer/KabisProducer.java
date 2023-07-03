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
    private final int clientId;

    public KabisProducer(Properties properties) {
        var bootstrapServers = properties.getProperty("bootstrap.servers").split(";");
        this.clientId = Integer.parseInt(properties.getProperty("client.clientId"));
        this.kafkaProducers = new ArrayList<>(bootstrapServers.length);
        for (int i = 0; i < bootstrapServers.length; i++) {
            String server = bootstrapServers[i];
            var id = String.format("%d-producer-%d", this.clientId, i);
            var simplerProperties = (Properties) properties.clone();
            simplerProperties.put("bootstrap.servers", server);
            simplerProperties.put("client.clientId", id);
            this.kafkaProducers.add(new KafkaProducer<>(simplerProperties));
        }
        this.serviceProxy = new KabisServiceProxy(this.clientId);
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
        var wrapper = new MessageWrapper<>(value, this.clientId);
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
                    SecureIdentifier sid = SecureIdentifier.factory(key, value, topic, partition, this.clientId);
                    serviceProxy.push(sid);
                }).join();
        CompletableFuture.allOf(completableFutures).join();
    }


    /**
     * Pushes the record to the first Kafka producer.
     * <br>
     * The record value is wrapped in a {@link MessageWrapper} and sent to the first Kafka producer.
     *
     * @param record the record to push
     */
    private void pushUnvalidated(ProducerRecord<K, V> record) {
        MessageWrapper<V> wrappedValue = new MessageWrapper<>(record.value());
        ProducerRecord<K, MessageWrapper<V>> wrappedRecord = new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), wrappedValue, record.headers());
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
