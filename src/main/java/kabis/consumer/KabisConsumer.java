package kabis.consumer;

import kabis.configs.KabisConsumerConfig;
import kabis.configs.properties_validators.KabisConsumerPropertiesValidator;
import kabis.validation.KabisServiceProxy;
import kabis.validation.SecureIdentifier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KabisConsumer<K extends Integer, V extends String> implements KabisConsumerI<K, V> {
    private final Set<String> validatedTopics = new HashSet<>();
    private final KabisServiceProxy serviceProxy;
    private final KafkaPollingThread<K, V> kafkaPollingThread;
    private final Validator<K, V> validator;
    private final Logger log;
    private final List<TopicPartition> assignedPartitions = new ArrayList<>();
    //TODO: REMOVE THIS
    public int counter = 0;

    /**
     * Creates a new KabisConsumer.
     *
     * @param properties the properties to be used by the Kabis consumer
     */
    public KabisConsumer(Properties properties) {
        this.log = LoggerFactory.getLogger(KabisConsumer.class);
        properties = KabisConsumerPropertiesValidator.getInstance().validate(properties);
        int clientId = Integer.parseInt(properties.getProperty(KabisConsumerConfig.CLIENT_ID_CONFIG));
        this.serviceProxy = KabisServiceProxy.getInstance();
        boolean orderedPulls = Boolean.parseBoolean(properties.getProperty(KabisConsumerConfig.ORDERED_PULLS_CONFIG));
        this.serviceProxy.init(clientId, orderedPulls);
        this.kafkaPollingThread = new KafkaPollingThread<>(properties);
        this.validator = new Validator<>(this.kafkaPollingThread);
    }

    /**
     * Subscribes to a collection of topics.
     * Topic subscriptions are not incremental. The given list will replace the current assignment (if there is one).
     *
     * @param topics the topics to subscribe to
     */
    @Override
    public void subscribe(Collection<String> topics) {
        this.kafkaPollingThread.subscribe(topics);
        this.log.info("Subscribed to topic(s): {}", Utils.join(topics, ", "));
    }

    /**
     * Unsubscribes from all topics.
     */
    @Override
    public void unsubscribe() {
        this.kafkaPollingThread.unsubscribe();
        this.log.info("Unsubscribed from all topics");
    }

    /**
     * Polls for records.
     *
     * @param duration The maximum time to block (must not be greater than Long.MAX_VALUE milliseconds)
     * @return the records
     */
    public ConsumerRecords<K, V> poll(Duration duration) {
        List<SecureIdentifier> sids = this.serviceProxy.pull(this.assignedPartitions);
        System.out.printf("[" + this.getClass().getName() + "] Received %d sids%n", sids.size());
        // TODO: Remove this filter?
        sids = sids.stream().filter(sid -> this.assignedPartitions.contains(sid.getTopicPartition())).collect(Collectors.toList());
        System.out.printf("[" + this.getClass().getName() + "] After filter, received %d sids%n", sids.size());
        //TODO: Remove counter!
        this.counter += sids.size();
        System.out.println("[" + this.getClass().getName() + "] Total filtered SIDS until now: " + this.counter);

        Map<TopicPartition, List<ConsumerRecord<K, V>>> validatedRecords = this.validator.verify(sids, duration);
        Map<TopicPartition, List<ConsumerRecord<K, V>>> unvalidatedRecords = this.kafkaPollingThread.pollUnvalidated(this.validatedTopics, duration);

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
        this.kafkaPollingThread.close();
        this.serviceProxy.resetNextPullIndex();
        this.log.info("Consumer closed successfully");
    }

    /**
     * Close the consumer, waiting for up to the specified timeout for any needed cleanup.
     *
     * @param duration The time to wait for cleanup to finish
     */
    @Override
    public void close(Duration duration) {
        this.kafkaPollingThread.close(duration);
        this.serviceProxy.resetNextPullIndex();
        this.log.info("Consumer closed successfully");
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
        this.log.info("Updated list of validated topics: {}", Utils.join(validatedTopics, ", "));
        this.assignedPartitions.clear();
        this.assignedPartitions.addAll(this.kafkaPollingThread.getAssignedPartitions());
        this.log.info("Updated list of assigned partitions: {}", Utils.join(assignedPartitions, ", "));
    }

    /**
     * Returns the list of assigned partitions to the consumer.
     *
     * @return the list of assigned partitions
     */
    public List<TopicPartition> getAssignedPartitions() {
        return this.assignedPartitions;
    }
}