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
        if (properties == null || properties.isEmpty()) {
            throw new IllegalArgumentException("Properties cannot be null or empty");
        }
        if (properties.containsKey("key.deserializer")) {
            String keyDeserializer = properties.getProperty("key.deserializer");
            if (!keyDeserializer.matches("org\\.apache\\.kafka\\.common\\.serialization\\.IntegerDeserializer")) {
                throw new IllegalArgumentException("key.deserializer property must match org.apache.kafka.common.serialization.IntegerDeserializer");
            }
        }
        if (properties.containsKey("value.deserializer")) {
            String valueDeserializer = properties.getProperty("value.deserializer");
            if (!valueDeserializer.matches("kabis\\.storage\\.StringMessageWrapperDeserializer")) {
                throw new IllegalArgumentException("value.deserializer property must match kabis.storage.StringMessageWrapperDeserializer");
            }
        }
        if (properties.containsKey("key.serializer")) {
            String keySerializer = properties.getProperty("key.serializer");
            if (!keySerializer.matches("org\\.apache\\.kafka\\.common\\.serialization\\.IntegerSerializer")) {
                throw new IllegalArgumentException("key.serializer property must match org.apache.kafka.common.serialization.IntegerSerializer");
            }
        }
        if (properties.containsKey("value.serializer")) {
            String valueSerializer = properties.getProperty("value.serializer");
            if (!valueSerializer.matches("kabis\\.storage\\.StringMessageWrapperSerializer")) {
                throw new IllegalArgumentException("value.serializer property must match kabis.storage.StringMessageWrapperSerializer");
            }
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
