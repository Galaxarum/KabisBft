package kabis.configs.properties_validators;

import kabis.consumer.KabisConsumer;
import kabis.producer.KabisProducer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Validates properties that are in common between {@link KabisConsumer} and {@link KabisProducer}.
 */
public class KabisPropertiesValidator {
    private final Logger log = LoggerFactory.getLogger(KabisPropertiesValidator.class);

    /**
     * Validates the given properties, common to {@link KabisConsumer} and {@link KabisProducer} instances.
     *
     * @param properties the properties to be validated
     * @return the validated properties
     * @throws IllegalArgumentException if the properties are invalid
     */
    public Properties validate(Properties properties) {
        if (properties == null || properties.isEmpty()) {
            throw new IllegalArgumentException("Properties cannot be null or empty");
        }
        if (properties.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
            String bootstrapServers = properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalArgumentException("bootstrap.servers property cannot be null or empty");
            }
            if (!bootstrapServers.matches("([\\w\\d\\.]+:\\d+)([;,][\\w\\d\\.]+:\\d+)*")) {
                throw new IllegalArgumentException("bootstrap.servers property must be a list of host:port separated by commas and semicolons (e.g. host1:port1,host2:port2;host3:port3,host4:port4)");
            }
        }
        if (!properties.containsKey(CommonClientConfigs.CLIENT_ID_CONFIG)) {
            throw new IllegalArgumentException("Properties must contain client.id");
        } else {
            String clientId = properties.getProperty(CommonClientConfigs.CLIENT_ID_CONFIG);
            if (clientId == null || clientId.isEmpty()) {
                throw new IllegalArgumentException("client.id property cannot be null or empty");
            }
            if (!clientId.matches("\\d+")) {
                throw new IllegalArgumentException("client.id property must be a number");
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
        } else {
            this.log.warn("ordered.pulls property not found, using default value: true");
            properties.put("ordered.pulls", "true");
        }
        return properties;
    }
}

