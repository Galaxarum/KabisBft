package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class Ensure extends ArtExhibitionConsumer {
    public Ensure(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    private void run() {
        Security.addProvider(new BouncyCastleProvider());
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        properties.setProperty("client.id", String.valueOf(getClientId()));

        KabisConsumer<Integer, String> ensureConsumer = new KabisConsumer<>(properties);
        ensureConsumer.subscribe(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        ensureConsumer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.println("[Ensure] Kabis Consumer created");

        System.out.println("[Ensure] Reading alarms");
        // * getNumberOfArtExhibitions() will be removed when scaling on multiple consumers within the same consumer group,
        // every consumer will only read its own exhibition
        int recordsToRead = ((getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * 2 + getNumberOfUncaughtBreaches()) * getNumberOfArtExhibitions();
        //int recordsToReadWithoutSafeCorp = (getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * getNumberOfArtExhibitions();

        long time = pollAndMeasure(ensureConsumer, recordsToRead);
        ensureConsumer.close();

        System.out.println("[Ensure] DONE! Consumer Closed - Saving experiments");

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TOTAL ALARMS", "TOTAL TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(recordsToRead), Long.toString(time)));
        System.out.println("[Ensure] Experiments persisted!");
    }

    public static void main(String[] args) throws InterruptedException {
        // -- CHECK IF ALL ARGUMENTS ARE PRESENT --
        if (args.length != 5) {
            System.out.print("--ERROR-- \nUSAGE: Ensure <clientId> <numberOfArtExhibitions> <totalNumberOfAlarms> <falseAlarmsPercentage> <alarmsNotTriggeredPercentage>");
            System.exit(1);
        }
        new Ensure(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3]), parseInt(args[4])).run();
    }
}
