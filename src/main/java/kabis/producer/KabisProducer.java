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

    /**
     * Creates a new KabisProducer.
     *
     * @param properties the properties to be used by the Kabis producer
     */
    public KabisProducer(Properties properties) {
        //TODO: Improve the regex + check if the properties are valid, otherwise throw an exception
        String[] bootstrapServers = properties.getProperty("bootstrap.servers").split(";");
        this.clientId = Integer.parseInt(properties.getProperty("client.id"));
        this.kafkaProducers = new ArrayList<>(bootstrapServers.length);
        for (int i = 0; i < bootstrapServers.length; i++) {
            String server = bootstrapServers[i];
            String id = String.format("%d-producer-%d", this.clientId, i);
            Properties simplerProperties = (Properties) properties.clone();
            simplerProperties.put("bootstrap.servers", server);
            simplerProperties.put("client.id", id);
            this.kafkaProducers.add(new KafkaProducer<>(simplerProperties));
        }
        this.serviceProxy = new KabisServiceProxy(this.clientId);
    }

    /**
     * Updates the list of validated topics.
     *
     * @param validatedTopics the new list of validated topics
     */
    @Override
    public void updateTopology(Collection<String> validatedTopics) {
        synchronized (this.validatedTopics) {
            this.validatedTopics.clear();
            this.validatedTopics.addAll(validatedTopics);
        }
    }

    /**
     * Pushes a record to the Kafka cluster.
     *
     * @param record the record to be pushed
     */
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

    /**
     * Pushes the record to all Kafka producers.
     * <br>
     * The record value is wrapped in a {@link MessageWrapper} and sent to all Kafka producers.
     * <br>
     * The {@link KabisServiceProxy} is used to push the {@link SecureIdentifier}.
     *
     * @param record the record to push
     */
    @SuppressWarnings("unchecked")
    private void pushValidated(ProducerRecord<K, V> record) {
        V value = record.value();
        K key = record.key();
        MessageWrapper<V> wrappedValue = new MessageWrapper<>(value, this.clientId);
        ProducerRecord<K, MessageWrapper<V>> wrappedRecord = new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), wrappedValue, record.headers());
        //TODO: Remove this print
        System.out.println("Pushing validated record: " + wrappedRecord);

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
                    String topic = metadata.topic();
                    int partition = metadata.partition();
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

    /**
     * Flushes all Kafka producers.
     */
    @Override
    public void flush() {
        kafkaProducers.forEach(KafkaProducer::flush);
    }

    /**
     * Closes all Kafka producers.
     */
    @Override
    public void close() {
        kafkaProducers.forEach(KafkaProducer::close);
    }

    /**
     * Closes all Kafka producers.
     *
     * @param duration the duration to wait for the close operation to complete
     */
    @Override
    public void close(Duration duration) {
        kafkaProducers.forEach(p -> p.close(duration));
    }
}
