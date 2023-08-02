package kabis.consumer;

import kabis.configs.KabisPartitionAssignorConfig;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A custom partition assignor for Kabis. It assigns partitions to consumers in a group based on the group consumers ids
 * specified by the user in the properties file. The result of the assignment also depends on the number of partitions
 * for each topic.
 * <br>
 * <br>
 * The KabisPartitionAssignor guarantees that in different replicas, the same consumer group will have the same partition
 * assignments.
 */
public class KabisPartitionAssignor extends AbstractPartitionAssignor implements Configurable {
    public static final String KABIS_ASSIGNOR_NAME = "kabis-assignor";
    private final Logger log = LoggerFactory.getLogger(KabisPartitionAssignor.class);
    /**
     * The configuration for the KabisPartitionAssignor.
     */
    private KabisPartitionAssignorConfig config;

    /**
     * Returns the name of the KabisPartitionAssignor.
     *
     * @return The name of the KabisPartitionAssignor
     */
    @Override
    public String name() {
        return KABIS_ASSIGNOR_NAME;
    }

    /**
     * Assigns partitions to consumers in a group. To make the assignment, this function utilizes the {@link KabisPartitionAssignorConfig}
     * specified by the user in the properties file.
     *
     * @param partitionsPerTopic The number of partitions for each subscribed topic. Topics not in metadata will be excluded
     *                           from this map.
     * @param subscriptions      Map from the member id to their respective topic subscription
     * @return Map from each member to the list of partitions assigned to them.
     */
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        this.log.info("Assigning partitions to consumers using KabisPartitionAssignor");
        Map<String, List<TopicPartition>> partitionsPerConsumerId = new HashMap<>();
        Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);
        this.log.info("Consumers per topic: {}", consumersPerTopic);

        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<>());

        for (Map.Entry<String, List<MemberInfo>> topicEntry : consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<MemberInfo> consumersForTopic = topicEntry.getValue();

            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null)
                continue;

            String[] consumerIds = this.config.groupConsumersIds();
            int numberOfConsumersPerGroup = consumerIds.length;

            int numPartitionsPerConsumer = numPartitionsForTopic / numberOfConsumersPerGroup;
            int consumersWithExtraPartition = numPartitionsForTopic % numberOfConsumersPerGroup;
            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);

            for (int i = 0, n = consumerIds.length; i < n; i++) {
                int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);
                int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);
                partitionsPerConsumerId.putIfAbsent(consumerIds[i], new ArrayList<>());
                partitionsPerConsumerId.get(consumerIds[i]).addAll(partitions.subList(start, start + length));
            }
            this.log.info("Partitions per consumer id: {}", partitionsPerConsumerId);


            for (MemberInfo memberInfo : consumersForTopic) {
                String consumerId = memberInfo.groupInstanceId.orElse(null);
                if (consumerId == null) {
                    this.log.error("Consumer {} does not have group instance id, skipping", memberInfo.memberId);
                    continue;
                }
                consumerId = consumerId.substring(consumerId.lastIndexOf("-") + 1);
                assignment.get(memberInfo.memberId).addAll(partitionsPerConsumerId.get(consumerId));
            }
            this.log.info("Assignment: {}", assignment);
        }
        return assignment;
    }

    /**
     * Returns the list of consumers per topic.
     *
     * @param consumerMetadata Map from the member id to their respective topic subscription.
     * @return Map from each topic to the list of consumers subscribed to it.
     */
    private Map<String, List<MemberInfo>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<MemberInfo>> topicToConsumers = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            MemberInfo memberInfo = new MemberInfo(consumerId, subscriptionEntry.getValue().groupInstanceId());
            for (String topic : subscriptionEntry.getValue().topics()) {
                put(topicToConsumers, topic, memberInfo);
            }
        }
        return topicToConsumers;
    }

    /**
     * Gets the configs specified by the user in the properties file.
     *
     * @param configs The map of configs specified in the properties file.
     */
    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new KabisPartitionAssignorConfig(configs);
    }
}
