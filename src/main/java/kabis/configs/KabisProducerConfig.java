package kabis.configs;

import kabis.producer.KabisProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Properties;

/**
 * A custom config for the {@link KabisProducer}.
 */
public class KabisProducerConfig extends ProducerConfig {
    public static final String ORDERED_PULLS_CONFIG = "ordered.pulls";
    public static final String ORDERED_PULLS_DOC = "If true, the consumer will pull the messages in order.";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(ORDERED_PULLS_CONFIG, ConfigDef.Type.BOOLEAN, true,
                        ConfigDef.Importance.MEDIUM, ORDERED_PULLS_DOC);
    }

    public KabisProducerConfig(Properties props) {
        super(props);
    }
}
