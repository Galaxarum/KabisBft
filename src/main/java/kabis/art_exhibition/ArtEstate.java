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
    public ArtEstate(Integer numberOfArtExhibition, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(numberOfArtExhibition, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    private void run() {
        Security.addProvider(new BouncyCastleProvider());
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        properties.setProperty("client.id", "-Ensure");

        KabisConsumer<Integer, String> artEstateConsumer = new KabisConsumer<>(properties);
        artEstateConsumer.subscribe(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        artEstateConsumer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.printf("[ArtEstate] Kabis Consumer created/n");

        System.out.printf("[ArtEstate] Reading alarms\n");
        int recordsToRead = ((getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * 2 + getNumberOfUncaughtBreaches()) * getNumberOfArtExhibitions();
        long time = pollAndMeasure(artEstateConsumer, recordsToRead);
        artEstateConsumer.close();

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of TOTAL ALARMS", "Total TIME [ns]"),
                Arrays.asList(Integer.toString(recordsToRead), Long.toString(time)));
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.print("--ERROR-- \nUSAGE: ArtEstate <numberOfArtExhibition> <totalNumberOfAlarms> <falseAlarmsPercentage> <alarmsNotTriggeredPercentage>");
            System.exit(0);
        }

        new ArtEstate(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }
}
