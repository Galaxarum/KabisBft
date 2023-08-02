package kabis.configs;

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
        if (properties.containsKey("bootstrap.servers")) {
            String bootstrapServers = properties.getProperty("bootstrap.servers");
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalArgumentException("bootstrap.servers property cannot be null or empty");
            }
            if (!bootstrapServers.matches("([\\w\\d\\.]+:\\d+)([;,][\\w\\d\\.]+:\\d+)*")) {
                throw new IllegalArgumentException("bootstrap.servers property must be a list of host:port separated by commas and semicolons (e.g. host1:port1,host2:port2;host3:port3,host4:port4)");
            }
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
