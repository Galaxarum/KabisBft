package kabis.configs;

import kabis.consumer.KabisPartitionAssignor;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * A custom config for the {@link KabisPartitionAssignor}. It contains the list of consumers ids in the group.
 */
public class KabisPartitionAssignorConfig extends AbstractConfig {
    public static final String GROUP_CONSUMERS_IDS_CONFIG = "group.consumers.ids";
    public static final String GROUP_CONSUMERS_IDS_DOC = "List of consumers ids in the group, comma separated";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
                .define(GROUP_CONSUMERS_IDS_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, GROUP_CONSUMERS_IDS_DOC);
    }

    public KabisPartitionAssignorConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public String[] groupConsumersIds() {
        return getString(GROUP_CONSUMERS_IDS_CONFIG).split(",");
    }
}
