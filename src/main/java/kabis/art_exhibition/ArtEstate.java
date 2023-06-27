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
    public ArtEstate(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    private void run() {
        Security.addProvider(new BouncyCastleProvider());
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        properties.setProperty("client.id", String.valueOf(getClientId()));

        KabisConsumer<Integer, String> artEstateConsumer = new KabisConsumer<>(properties);
        artEstateConsumer.subscribe(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        artEstateConsumer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.println("[ArtEstate] Kabis Consumer created");

        System.out.println("[ArtEstate] Reading alarms");

        int recordsToRead = ((getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * 2 + getNumberOfUncaughtBreaches()) * getNumberOfArtExhibitions();
        //int recordsToReadWithoutSafeCorp = (getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * getNumberOfArtExhibitions();

        long time = pollAndMeasure(artEstateConsumer, recordsToRead);
        artEstateConsumer.close();

        System.out.println("[ArtEstate] DONE! Consumer Closed - Saving experiments");

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TOTAL ALARMS", "TOTAL TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(recordsToRead), Long.toString(time)));
        System.out.println("[ArtEstate] Experiments persisted!");
    }

    public static void main(String[] args) throws InterruptedException {
        // -- CHECK IF ALL ARGUMENTS ARE PRESENT --
        if (args.length != 5) {
            System.out.print("--ERROR-- \nUSAGE: ArtEstate <clientId> <numberOfArtExhibitions> <totalNumberOfAlarms> <falseAlarmsPercentage> <alarmsNotTriggeredPercentage>");
            System.exit(1);
        }
        new ArtEstate(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3]), parseInt(args[4])).run();
    }
}
