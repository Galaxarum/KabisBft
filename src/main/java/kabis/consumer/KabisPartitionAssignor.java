package kabis.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class KabisPartitionAssignor extends AbstractPartitionAssignor implements Configurable {
    private final Logger log = LoggerFactory.getLogger(KabisPartitionAssignor.class);
    public static final String KABIS_ASSIGNOR_NAME = "kabis-assignor";

    private KabisPartitionAssignorConfig config = new KabisPartitionAssignorConfig(new HashMap<>());

    @Override
    public String name() {
        return KABIS_ASSIGNOR_NAME;
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
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);
        int numberOfConsumersPerGroup = 5;

        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<>());

        for (Map.Entry<String, List<MemberInfo>> topicEntry : consumersPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<MemberInfo> consumersForTopic = topicEntry.getValue();

            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null)
                continue;

            Collections.sort(consumersForTopic);

            int numPartitionsPerConsumer = numPartitionsForTopic / numberOfConsumersPerGroup;
            int consumersWithExtraPartition = numPartitionsForTopic % numberOfConsumersPerGroup;
            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);

            for (int i = 0, n = consumersForTopic.size(); i < n; i++) {
                int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);
                int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);
                assignment.get(consumersForTopic.get(i).memberId).addAll(partitions.subList(start, start + length));
            }
        }
        return assignment;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new KabisPartitionAssignorConfig(configs);
        this.log.info("Configured KabisPartitionAssignor with {}", this.config.groupConsumersIds());
    }
}

class KabisPartitionAssignorConfig extends AbstractConfig {
    public static final String GROUP_CONSUMERS_IDS_CONFIG = "group.consumers.ids";
    public static final String GROUP_CONSUMERS_IDS_DOC = "List of consumers ids in the group";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(GROUP_CONSUMERS_IDS_CONFIG, ConfigDef.Type.INT, Integer.MAX_VALUE,
                        ConfigDef.Importance.HIGH, GROUP_CONSUMERS_IDS_DOC);
    }

    public KabisPartitionAssignorConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public List<Integer> groupConsumersIds() {
        String groupConsumersIds = getString(GROUP_CONSUMERS_IDS_CONFIG);
        return Arrays.stream(groupConsumersIds.split(",")).map(Integer::parseInt).collect(Collectors.toList());
    }
}
