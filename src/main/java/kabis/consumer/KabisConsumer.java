package kabis.consumer;

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
    //TODO: REMOVE THIS
    public int counter = 0;

    private final List<TopicPartition> assignedPartitions = new ArrayList<>();
    private Boolean rebalanceNeeded = true;

    /**
     * Creates a new KabisConsumer.
     *
     * @param properties the properties to be used by the Kabis consumer
     */
    public KabisConsumer(Properties properties) {
        this.log = LoggerFactory.getLogger(KabisConsumer.class);
        //TODO: Check if the properties are valid, otherwise throw an exception
        int clientId = Integer.parseInt(properties.getProperty("client.id"));
        // TODO: Add orderedPulls support
        this.serviceProxy = KabisServiceProxy.getInstance();
        this.serviceProxy.init(clientId, false);
        this.kafkaPollingThread = new KafkaPollingThread<>(properties, this);
        this.validator = new Validator<>(this.kafkaPollingThread);
    }

    /**
     * Subscribes to a collection of topics.
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
    private ConsumerRecords<K, V> pollRecords(Duration duration) {
        List<SecureIdentifier> sids = this.serviceProxy.pull();
        System.out.printf("[" + this.getClass().getName() + "] Received %d sids%n", sids.size());
        //TODO: Remove counter!
        this.counter += sids.size();
        System.out.println("[" + this.getClass().getName() + "] Total SIDS until now: " + this.counter);

        // TODO: Why is duration not passed to the validator? And a new one is created?
        Map<TopicPartition, List<ConsumerRecord<K, V>>> validatedRecords = this.validator.verify(sids);
        //System.out.printf("[" + this.getClass().getName() + "] Received %d validated records%n", validatedRecords.values().stream().map(List::size).reduce(Integer::sum).orElse(-1));
        //if (!validatedRecords.isEmpty())
        //System.out.println("[" + this.getClass().getName() + "] Validated records: " + validatedRecords.values());
        Map<TopicPartition, List<ConsumerRecord<K, V>>> unvalidatedRecords = this.kafkaPollingThread.pollUnvalidated(this.validatedTopics, duration);

        Map<TopicPartition, List<ConsumerRecord<K, V>>> mergedMap = Stream.concat(validatedRecords.entrySet().stream(), unvalidatedRecords.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (l1, l2) -> Stream.concat(l1.stream(), l2.stream()).collect(Collectors.toList())
                        )
                );

        return new ConsumerRecords<>(mergedMap);
    }

    /**
     * Polls for records.
     *
     * @param duration The maximum time to block (must not be greater than Long.MAX_VALUE milliseconds)
     * @return the records
     */
    @Override
    public ConsumerRecords<K, V> poll(Duration duration) {


        return new ConsumerRecords<>(mergedMap);
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     */
    @Override
    public void close() {
        this.kafkaPollingThread.close();
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
    }

    /**
     * Empties the list of validated topics and updates it with the new list.
     *
     * @param validatedTopics the new list of validated topics
     */
    @Override
    public void updateTopology(Collection<String> validatedTopics) {
        synchronized (this.validatedTopics) {
            this.validatedTopics.clear();
            this.validatedTopics.addAll(validatedTopics);
        }
        this.rebalanceNeeded = true;
        this.log.info("Updated list of validated topics: {}", Utils.join(validatedTopics, ", "));
        this.kafkaPollingThread.fetchPartitions();
    }

    /**
     * Updates the list of assigned partitions.
     *
     * @param assignedPartitions the new list of assigned partitions
     */
    public void updateAssignedPartitions(List<TopicPartition> assignedPartitions) {
        synchronized (this.assignedPartitions) {
            this.assignedPartitions.clear();
            this.assignedPartitions.addAll(assignedPartitions);
        }
        this.rebalanceNeeded = false;
        this.log.info("Updated list of assigned partitions: {}", Utils.join(assignedPartitions, ", "));
    }

    public void setRebalanceNeeded() {
        this.rebalanceNeeded = true;

    }
}
