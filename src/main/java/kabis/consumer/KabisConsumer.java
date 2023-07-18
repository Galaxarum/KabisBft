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
    private final PollingThread<K, V> pollingThread;
    private List<SecureIdentifier> pulledSids = new ArrayList<>();

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
        this.pollingThread = new PollingThread<>(this);
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
    @Override
    public ConsumerRecords<K, V> poll(Duration duration) {
        if (this.rebalanceNeeded) return new ConsumerRecords<>(new HashMap<>());

        this.pollingThread.setDuration(duration);
        this.pollingThread.start();
        try {
            this.pollingThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return this.pollingThread.getRecords();
    }

    /**
     * Polls for records.
     *
     * @param duration The maximum time to block (must not be greater than Long.MAX_VALUE milliseconds)
     * @return the records
     */
    protected ConsumerRecords<K, V> pollRecords(Duration duration) {
        if (this.pulledSids.isEmpty()) this.pulledSids = this.serviceProxy.pull();
        this.pulledSids = this.pulledSids.stream().filter(sid -> this.assignedPartitions.contains(sid.getTopicPartition())).collect(Collectors.toList());

        System.out.printf("[" + this.getClass().getName() + "] Received %d sids%n", this.pulledSids.size());
        //TODO: Remove counter!
        this.counter += this.pulledSids.size();
        System.out.println("[" + this.getClass().getName() + "] Total SIDS until now: " + this.counter);

        // TODO: Why is duration not passed to the validator? And a new one is created?
        Map<TopicPartition, List<ConsumerRecord<K, V>>> validatedRecords = this.validator.verify(this.pulledSids);
        //System.out.printf("[" + this.getClass().getName() + "] Received %d validated records%n", validatedRecords.values().stream().map(List::size).reduce(Integer::sum).orElse(-1));
        //if (!validatedRecords.isEmpty())
        //System.out.println("[" + this.getClass().getName() + "] Validated records: " + validatedRecords.values());
        Map<TopicPartition, List<ConsumerRecord<K, V>>> unvalidatedRecords = this.kafkaPollingThread.pollUnvalidated(this.validatedTopics, duration);

        Map<TopicPartition, List<ConsumerRecord<K, V>>> mergedMap = Stream.concat(validatedRecords.entrySet().stream(), unvalidatedRecords.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (l1, l2) -> Stream.concat(l1.stream(), l2.stream()).collect(Collectors.toList())
                        )
                );

        this.pulledSids.clear();
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
        this.pollingThread.interrupt();
    }
}

class PollingThread<K extends Integer, V extends String> extends Thread {
    private Duration duration;
    private final KabisConsumer<K, V> kabisConsumer;
    private ConsumerRecords<K, V> records;
    private final Object recordsLock = new Object();

    public PollingThread(KabisConsumer<K, V> kabisConsumer) {
        this.kabisConsumer = kabisConsumer;
        this.duration = Duration.ofSeconds(1);
    }

    public void setDuration(Duration duration) {
        this.duration = duration;
    }

    public void run() {
        synchronized (this.recordsLock) {
            this.records = this.kabisConsumer.pollRecords(this.duration);
        }
    }

    public ConsumerRecords<K, V> getRecords() {
        synchronized (this.recordsLock) {
            return this.records;
        }
    }
}