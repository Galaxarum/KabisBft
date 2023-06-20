package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class ArtEstate extends ArtExhibitionConsumer {
    public ArtEstate(Integer artExhibitionID, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(artExhibitionID, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    private void run() {
        Security.addProvider(new BouncyCastleProvider());
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        properties.setProperty("client.id", getArtExhibitionID() + "-Ensure");

        KabisConsumer<Integer, String> artEstateConsumer = new KabisConsumer<>(properties);
        artEstateConsumer.subscribe(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        artEstateConsumer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.printf("[%s-ArtEstate] Kabis Consumer created/n", getArtExhibitionID());

        System.out.printf("[%s-ArtEstate] Reading alarms\n", getArtExhibitionID());
        long time = pollAndMeasure(artEstateConsumer, (getNumberOfTrueAlarms() * 2) + getNumberOfFalseAlarms() + getNumberOfUncaughtBreaches());
        artEstateConsumer.close();

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of TOTAL ALARMS", "Total TIME [ns]"),
                Arrays.asList(Integer.toString((getNumberOfTrueAlarms() * 2) + getNumberOfFalseAlarms() + getNumberOfUncaughtBreaches()), Long.toString(time)));
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.print("--ERROR-- \nUSAGE: ArtEstate <artExhibitionID> <totalNumberOfAlarms> <falseAlarmsPercentage> <alarmsNotTriggeredPercentage>");
            System.exit(0);
        }

        new ArtEstate(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }
}
