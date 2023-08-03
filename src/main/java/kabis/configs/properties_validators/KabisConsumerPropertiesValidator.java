package kabis.configs.properties_validators;

import kabis.configs.KabisConsumerConfig;
import kabis.configs.KabisPartitionAssignorConfig;
import kabis.consumer.KabisConsumer;
import kabis.consumer.KabisPartitionAssignor;
import kabis.storage.StringMessageWrapperDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Validates {@link KabisConsumer} properties.
 */
public class KabisConsumerPropertiesValidator extends KabisPropertiesValidator {
    private static final KabisConsumerPropertiesValidator instance = new KabisConsumerPropertiesValidator();

    private final Logger log = LoggerFactory.getLogger(KabisConsumerPropertiesValidator.class);

    public static KabisConsumerPropertiesValidator getInstance() {
        return instance;
    }

    /**
     * Validates the given properties, for a {@link KabisConsumer} instance.
     *
     * @param properties the properties to be validated
     * @return the validated properties
     * @throws IllegalArgumentException if the properties are invalid
     */
    @Override
    public Properties validate(Properties properties) {
        properties = super.validate(properties);
        if (properties.containsKey(KabisConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            // Validate the key.deserializer property, the only valid value is org.apache.kafka.common.serialization.IntegerDeserializer
            String keyDeserializer = properties.getProperty(KabisConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
            if (!keyDeserializer.matches("org\\.apache\\.kafka\\.common\\.serialization\\.IntegerDeserializer")) {
                throw new IllegalArgumentException("key.deserializer property must match org.apache.kafka.common.serialization.IntegerDeserializer");
            }
        } else {
            this.log.warn("key.deserializer property is not set. Defaulting to org.apache.kafka.common.serialization.IntegerDeserializer");
            properties.put(KabisConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        }

        if (properties.containsKey(KabisConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            // Validate the value.deserializer property, the only valid value is kabis.storage.StringMessageWrapperDeserializer
            String valueDeserializer = properties.getProperty(KabisConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
            if (!valueDeserializer.matches("kabis\\.storage\\.StringMessageWrapperDeserializer")) {
                throw new IllegalArgumentException("value.deserializer property must match kabis.storage.StringMessageWrapperDeserializer");
            }
        } else {
            this.log.warn("value.deserializer property is not set. Defaulting to kabis.storage.StringMessageWrapperDeserializer");
            properties.put(KabisConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringMessageWrapperDeserializer.class.getName());
        }

        if (properties.containsKey(KabisConsumerConfig.GROUP_ID_CONFIG)) {
            // Set the group.instance.id property
            int clientId = Integer.parseInt(properties.getProperty(KabisConsumerConfig.CLIENT_ID_CONFIG));
            properties.put(KabisConsumerConfig.GROUP_INSTANCE_ID_CONFIG, String.format("%s-%d", properties.getProperty(KabisConsumerConfig.GROUP_ID_CONFIG), clientId));

            // Validate the group.consumers.ids property
            String groupConsumersIds = properties.getProperty(KabisPartitionAssignorConfig.GROUP_CONSUMERS_IDS_CONFIG);
            if (groupConsumersIds == null || groupConsumersIds.isEmpty()) {
                throw new IllegalArgumentException("group.consumers.ids property cannot be null or empty");
            }
            if (!groupConsumersIds.matches("\\d+(,\\d+)*")) {
                throw new IllegalArgumentException("group.consumers.ids property must be a list of numbers separated by commas");
            }

            // Validate the partition.assignment.strategy property
            if (properties.containsKey(KabisConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG)) {
                String partitionAssignmentStrategy = properties.getProperty(KabisConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
                if (!partitionAssignmentStrategy.matches("kabis\\.consumer\\.KabisPartitionAssignor")) {
                    throw new IllegalArgumentException("partition.assignment.strategy property must match kabis.consumer.KabisPartitionAssignor");
                }
            } else {
                this.log.warn("partition.assignment.strategy property is not set. Defaulting to kabis.consumer.KabisPartitionAssignor");
                properties.put(KabisConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, KabisPartitionAssignor.class.getName());
            }
        } else {
            throw new IllegalArgumentException("group.id property cannot be null or empty");
        }

        return properties;
    }
}
