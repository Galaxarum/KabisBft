package kabis.case_study;

import kabis.consumer.KabisConsumer;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class ArtEstate extends ArtExhibitionConsumer {
    public ArtEstate(String topic, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(topic, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    private void run() {
        Security.addProvider(new BouncyCastleProvider());
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        properties.setProperty("client.id", getTopic() + "-Ensure");

        KabisConsumer<Integer, String> artEstateConsumer = new KabisConsumer<>(properties);
        artEstateConsumer.subscribe(Collections.singletonList(getTopic()));
        artEstateConsumer.updateTopology(Collections.singletonList(getTopic()));
        System.out.printf("[%s-ArtEstate] Kabis Consumer created/n", getTopic());

        String message = "[ArtEstate] TRUE ALARM - Location: ";

        long time = pollAndMeasure(artEstateConsumer, (getNumberOfTrueAlarms() * 2) + getNumberOfFalseAlarms(), message);

        artEstateConsumer.close();
        CaseStudyBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of TRUE ALARMS", "Number of FALSE ALARMS", "Total TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfTrueAlarms()), Integer.toString(getNumberOfFalseAlarms()), Long.toString(time)));
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.print("--ERROR-- \nUSAGE: ArtEstate <topic> <totalNumberOfAlarms> <falseAlarmsPercentage> <alarmsNotTriggeredPercentage>");
            System.exit(0);
        }

        new ArtEstate(args[0], parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }
}
