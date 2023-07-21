package kabis.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KabisPartitionAssignor extends AbstractPartitionAssignor implements Configurable {
    public static final String KABIS_ASSIGNOR_NAME = "kabis-assignor";
    private final Logger log = LoggerFactory.getLogger(KabisPartitionAssignor.class);
    private KabisPartitionAssignorConfig config;

    @Override
    public String name() {
        return KABIS_ASSIGNOR_NAME;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        log.info("Assigning partitions to consumers using KabisPartitionAssignor");
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

            String[] consumerIds = config.groupConsumersIds();
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

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new KabisPartitionAssignorConfig(configs);
    }
}

class KabisPartitionAssignorConfig extends AbstractConfig {
    public static final String GROUP_CONSUMERS_IDS_CONFIG = "group.consumers.ids";
    public static final String GROUP_CONSUMERS_IDS_DOC = "List of consumers ids in the group, comma separated";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(GROUP_CONSUMERS_IDS_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, GROUP_CONSUMERS_IDS_DOC);
    }

    public KabisPartitionAssignorConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public String[] groupConsumersIds() {
        return getString(GROUP_CONSUMERS_IDS_CONFIG).split(",");
    }
}
