package kabis;

import kabis.consumer.KabisConsumer;
import kabis.producer.KabisProducer;

import java.util.Properties;

/**
 * Validates {@link KabisProducer} and {@link KabisConsumer} properties.
 */
public class PropertiesValidator {
    private static final PropertiesValidator instance = new PropertiesValidator();

    public static PropertiesValidator getInstance() {
        return instance;
    }

    /**
     * Validates the given properties.
     *
     * @param properties the properties to be validated
     * @throws IllegalArgumentException if the properties are invalid
     */
    public void validate(Properties properties) {
        //TODO: Validate key.deserializer, value.deserializer, key.serializer, value.serializer
        if (properties == null || properties.isEmpty()) {
            throw new IllegalArgumentException("Properties cannot be null or empty");
        }
        if (!properties.containsKey("bootstrap.servers")) {
            throw new IllegalArgumentException("Properties must contain bootstrap.servers");
        }
        if (!properties.containsKey("client.id")) {
            throw new IllegalArgumentException("Properties must contain client.id");
        } else {
            String clientId = properties.getProperty("client.id");
            if (clientId == null || clientId.isEmpty()) {
                throw new IllegalArgumentException("client.id property cannot be null or empty");
            }
            if (!clientId.matches("\\d+")) {
                throw new IllegalArgumentException("client.id property must be a number");
            }
        }
        if (properties.containsKey("group.id")) {
            String groupConsumersIds = properties.getProperty("group.consumers.ids");
            if (groupConsumersIds == null || groupConsumersIds.isEmpty()) {
                throw new IllegalArgumentException("group.consumers.ids property cannot be null or empty");
            }
            if (!groupConsumersIds.matches("\\d+(,\\d+)*")) {
                throw new IllegalArgumentException("group.consumers.ids property must be a list of numbers separated by commas");
            }

            String partitionAssignmentStrategy = properties.getProperty("partition.assignment.strategy");
            if (partitionAssignmentStrategy == null || partitionAssignmentStrategy.isEmpty()) {
                throw new IllegalArgumentException("partition.assignment.strategy property cannot be null or empty");
            }
            if (!partitionAssignmentStrategy.contains("KabisPartitionAssignor")) {
                throw new IllegalArgumentException("partition.assignment.strategy property must be KabisPartitionAssignor");
            }
        }
        if (properties.containsKey("ordered.pulls")) {
            String orderedPulls = properties.getProperty("ordered.pulls");
            if (orderedPulls == null || orderedPulls.isEmpty()) {
                throw new IllegalArgumentException("ordered.pulls property cannot be null or empty");
            }
            if (!orderedPulls.matches("true|false")) {
                throw new IllegalArgumentException("ordered.pulls property must be true or false");
            }
        }
    }
}
