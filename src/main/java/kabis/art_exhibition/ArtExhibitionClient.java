package kabis.art_exhibition;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.security.Security;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public abstract class ArtExhibitionClient {
    protected static final List<String> TOPICS = Collections.singletonList(Topics.ART_EXHIBITION.toString());

    protected ArtExhibitionClient() {
        Security.addProvider(new BouncyCastleProvider());
    }

    /**
     * Reads the properties file and returns the properties object,
     * by default it reads the config.properties file.
     *
     * @return Properties object
     */
    protected Properties readProperties() {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return properties;
    }

    /**
     * Reads the properties file and returns the properties object,
     *
     * @param propertiesFileName the name of the properties file
     * @return Properties object
     */
    protected Properties readProperties(String propertiesFileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(propertiesFileName));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return properties;
    }
}
