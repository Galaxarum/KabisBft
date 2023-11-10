package kabis.configs.properties_validators;

import kabis.configs.KabisProducerConfig;
import kabis.producer.KabisProducer;
import kabis.storage.StringMessageWrapperSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Validates {@link KabisProducer} properties.
 */
public class KabisProducerPropertiesValidator extends KabisPropertiesValidator {
    private static final KabisProducerPropertiesValidator instance = new KabisProducerPropertiesValidator();

    private final Logger log = LoggerFactory.getLogger(KabisProducerPropertiesValidator.class);

    public static KabisPropertiesValidator getInstance() {
        return instance;
    }

    /**
     * Validates the given properties, for a {@link KabisProducer} instance.
     *
     * @param properties the properties to be validated
     * @return the validated properties
     * @throws IllegalArgumentException if the properties are invalid
     */
    @Override
    public Properties validate(Properties properties) {
        properties = super.validate(properties);
        if (properties.containsKey(KabisProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            String keySerializer = properties.getProperty(KabisProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            if (!keySerializer.matches("org\\.apache\\.kafka\\.common\\.serialization\\.IntegerSerializer")) {
                throw new IllegalArgumentException("key.serializer property must match org.apache.kafka.common.serialization.IntegerSerializer");
            }
        } else {
            this.log.warn("key.serializer property is not set. Defaulting to org.apache.kafka.common.serialization.IntegerSerializer");
            properties.put(KabisProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        }
        if (properties.containsKey(KabisProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            String valueSerializer = properties.getProperty(KabisProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
            if (!valueSerializer.matches("kabis\\.storage\\.StringMessageWrapperSerializer")) {
                throw new IllegalArgumentException("value.serializer property must match kabis.storage.StringMessageWrapperSerializer");
            }
        } else {
            this.log.warn("value.serializer property is not set. Defaulting to kabis.storage.StringMessageWrapperSerializer");
            properties.put(KabisProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringMessageWrapperSerializer.class.getName());
        }
        return properties;
    }
}
