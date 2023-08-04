package kabis.producer;

import kabis.configs.KabisProducerConfig;
import kabis.configs.properties_validators.KabisProducerPropertiesValidator;
import kabis.storage.MessageWrapper;
import kabis.validation.KabisServiceProxy;
import kabis.validation.SecureIdentifier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class KabisProducer<K extends Integer, V extends String> implements KabisProducerI<K, V> {

    private final Set<String> validatedTopics = new HashSet<>();
    private final List<KafkaProducer<K, MessageWrapper<V>>> kafkaProducers;
    private final KabisServiceProxy serviceProxy;
    private final int clientId;
    private final Logger log;


    /**
     * Creates a new Kabis Producer.
     *
     * @param properties the properties to be used by the Kabis producer
     */
    public KabisProducer(Properties properties) {
        this.log = LoggerFactory.getLogger(KabisProducer.class);
        properties = KabisProducerPropertiesValidator.getInstance().validate(properties);
        String[] serversReplicas = properties.getProperty(KabisProducerConfig.BOOTSTRAP_SERVERS_CONFIG).split(";");
        this.clientId = Integer.parseInt(properties.getProperty(KabisProducerConfig.CLIENT_ID_CONFIG));
        this.kafkaProducers = new ArrayList<>(serversReplicas.length);
        for (int i = 0; i < serversReplicas.length; i++) {
            String servers = serversReplicas[i];
            String id = String.format("%d-producer-%d", this.clientId, i);
            Properties simplerProperties = (Properties) properties.clone();
            simplerProperties.put(KabisProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            simplerProperties.put(KabisProducerConfig.CLIENT_ID_CONFIG, id);
            this.kafkaProducers.add(new KafkaProducer<>(simplerProperties));
        }
        this.serviceProxy = KabisServiceProxy.getInstance();
        boolean orderedPulls = Boolean.parseBoolean(properties.getProperty(KabisProducerConfig.ORDERED_PULLS_CONFIG));
        this.serviceProxy.init(clientId, orderedPulls);
    }

    /**
     * Updates the list of validated topics.
     * The update of the topology is not incremental. The given list will replace the current assignment (if there is one).
     *
     * @param validatedTopics the new list of validated topics
     */
    @Override
    public void updateTopology(Collection<String> validatedTopics) {
        synchronized (this.validatedTopics) {
            this.validatedTopics.clear();
            this.validatedTopics.addAll(validatedTopics);
        }
        log.info("Updated topology: {}", Utils.join(validatedTopics, ", "));
    }

    /**
     * Pushes a record to the Kafka cluster.
     *
     * @param record the record to be pushed
     */
    @Override
    public void push(ProducerRecord<K, V> record) {
        synchronized (this.validatedTopics) {
            if (this.validatedTopics.contains(record.topic())) {
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
        ProducerRecord<K, MessageWrapper<V>> wrappedRecord = new ProducerRecord<>(record.topic(), record.partition(),
                record.timestamp(), record.key(), wrappedValue, record.headers());

        CompletableFuture<RecordMetadata>[] completableFutures = this.kafkaProducers.stream().map(prod -> {
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
                    this.serviceProxy.push(sid);
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
        ProducerRecord<K, MessageWrapper<V>> wrappedRecord = new ProducerRecord<>(record.topic(), record.partition(),
                record.timestamp(), record.key(), wrappedValue, record.headers());
        this.kafkaProducers.get(0).send(wrappedRecord);
    }

    /**
     * Flushes the producer.
     * Invoking this method makes all buffered records immediately available to send (even if linger.ms is greater than 0) and blocks on the completion of the requests associated with these records.
     */
    @Override
    public void flush() {
        this.kafkaProducers.forEach(KafkaProducer::flush);
    }

    /**
     * Closes the producer.
     * This method blocks until all previously sent requests complete.
     */
    @Override
    public void close() {
        this.kafkaProducers.forEach(KafkaProducer::close);
        this.log.info("Producer closed successfully");
    }

    /**
     * Closes the producer.
     * This method waits up to timeout for the producer to complete the sending of all incomplete requests.
     *
     * @param duration the duration to wait for the close operation to complete
     */
    @Override
    public void close(Duration duration) {
        this.kafkaProducers.forEach(p -> p.close(duration));
        this.log.info("Producer closed successfully");
    }
}
