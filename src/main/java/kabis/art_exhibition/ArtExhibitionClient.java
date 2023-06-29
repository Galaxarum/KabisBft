package kabis.art_exhibition;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.security.Security;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public abstract class ArtExhibitionClient {
    private Properties properties;

    protected static final List<String> TOPICS = Collections.singletonList(Topics.ART_EXHIBITION.toString());

    protected Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    protected ArtExhibitionClient() {
        Security.addProvider(new BouncyCastleProvider());
        this.properties = readProperties();
    }

    private Properties readProperties() {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return properties;
    }
}
