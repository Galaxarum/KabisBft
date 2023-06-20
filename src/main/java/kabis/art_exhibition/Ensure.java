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
    public Ensure(Integer artExhibitionID, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
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

        KabisConsumer<Integer, String> ensureConsumer = new KabisConsumer<>(properties);
        ensureConsumer.subscribe(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        ensureConsumer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.printf("[%s-Ensure] Kabis Consumer created/n", getArtExhibitionID());

        System.out.printf("[%s-Ensure] Reading alarms\n", getArtExhibitionID());
        long time = pollAndMeasure(ensureConsumer, (getNumberOfTrueAlarms() * 2) + getNumberOfFalseAlarms() + getNumberOfUncaughtBreaches());
        ensureConsumer.close();

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of TOTAL ALARMS", "Total TIME [ns]"),
                Arrays.asList(Integer.toString((getNumberOfTrueAlarms() * 2) + getNumberOfFalseAlarms() + getNumberOfUncaughtBreaches()), Long.toString(time)));
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.print("--ERROR-- \nUSAGE: Ensure <artExhibitionID> <totalNumberOfAlarms> <falseAlarmsPercentage> <alarmsNotTriggeredPercentage>");
            System.exit(0);
        }

        new Ensure(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }
}
