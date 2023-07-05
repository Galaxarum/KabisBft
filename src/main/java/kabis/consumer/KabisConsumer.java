package kabis.consumer;

import kabis.validation.KabisServiceProxy;
import kabis.validation.SecureIdentifier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KabisConsumer<K extends Integer, V extends String> implements KabisConsumerI<K, V> {

    private final Set<String> validatedTopics = new HashSet<>();
    private final KabisServiceProxy serviceProxy;
    private final KafkaPollingThread<K, V> kafkaPollingThread;
    private final Validator<K, V> validator;

    /**
     * Creates a new KabisConsumer.
     *
     * @param properties the properties to be used by the Kabis consumer
     */
    public KabisConsumer(Properties properties) {
        //TODO: Check if the properties are valid, otherwise throw an exception
        int clientId = Integer.parseInt(properties.getProperty("client.id"));
        this.serviceProxy = new KabisServiceProxy(clientId);
        this.kafkaPollingThread = new KafkaPollingThread<>(properties);
        this.validator = new Validator<>(kafkaPollingThread);
    }

    /**
     * Subscribes to a collection of topics.
     *
     * @param topics the topics to subscribe to
     */
    @Override
    public void subscribe(Collection<String> topics) {
        kafkaPollingThread.subscribe(topics);
    }

    /**
     * Unsubscribes from all topics.
     */
    @Override
    public void unsubscribe() {
        kafkaPollingThread.unsubscribe();
    }

    /**
     * Polls for records.
     *
     * @param duration The maximum time to block (must not be greater than Long.MAX_VALUE milliseconds)
     * @return the records
     */
    @Override
    public ConsumerRecords<K, V> poll(Duration duration) {
        //TODO: Remove all the prints
        List<SecureIdentifier> sids = serviceProxy.pull();
        System.out.printf("[" + this.getClass().getName() + "] Received %d sids%n", sids.size());

        Map<TopicPartition, List<ConsumerRecord<K, V>>> validatedRecords = validator.verify(sids);
        System.out.printf("[" + this.getClass().getName() + "] Received %d validated records%n", validatedRecords.values().stream().map(List::size).reduce(Integer::sum).orElse(-1));
        //if (!validatedRecords.isEmpty())
        //System.out.println("[" + this.getClass().getName() + "] Validated records: " + validatedRecords.values());
        Map<TopicPartition, List<ConsumerRecord<K, V>>> unvalidatedRecords = kafkaPollingThread.pollUnvalidated(validatedTopics, duration);

        Map<TopicPartition, List<ConsumerRecord<K, V>>> mergedMap = Stream.concat(validatedRecords.entrySet().stream(), unvalidatedRecords.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (l1, l2) -> Stream.concat(l1.stream(), l2.stream()).collect(Collectors.toList())
                        )
                );

        return new ConsumerRecords<>(mergedMap);
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     */
    @Override
    public void close() {
        kafkaPollingThread.close();
    }

    /**
     * Close the consumer, waiting for up to the specified timeout for any needed cleanup.
     *
     * @param duration The time to wait for cleanup to finish
     */
    @Override
    public void close(Duration duration) {
        kafkaPollingThread.close(duration);
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
}
