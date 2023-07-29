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
        if (properties.containsKey("group.consumers.ids")) {
            String groupConsumersIds = properties.getProperty("group.consumers.ids");
            if (groupConsumersIds == null || groupConsumersIds.isEmpty()) {
                throw new IllegalArgumentException("group.consumers.ids property cannot be null or empty");
            }
            if (!groupConsumersIds.matches("\\d+(,\\d+)*")) {
                throw new IllegalArgumentException("group.consumers.ids property must be a list of numbers separated by commas");
            }
        }
    }
}
