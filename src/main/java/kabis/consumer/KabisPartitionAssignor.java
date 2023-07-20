package kabis.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
        Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);

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

            List<MemberInfo> consumersList = new ArrayList<>();
            for (int i = 0; i < numberOfConsumersPerGroup; i++) {
                String consumerId = consumersForTopic.get(i).groupInstanceId.orElse(null);
                if (consumerId == null)
                    continue;
                consumerId = consumerId.substring(consumerId.lastIndexOf("-") + 1);
            }

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
