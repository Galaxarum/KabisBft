package kabis.case_study;

import kabis.consumer.KabisConsumer;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class Ensure extends ArtExhibitionConsumer {
    public Ensure(String topic, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
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

        KabisConsumer<Integer, String> ensureConsumer = new KabisConsumer<>(properties);
        ensureConsumer.subscribe(Collections.singletonList(getTopic()));
        ensureConsumer.updateTopology(Collections.singletonList(getTopic()));
        System.out.printf("[%s-Ensure] Kabis Consumer created/n", getTopic());

        System.out.printf("[%s-Ensure] Reading alarms\n", getTopic());
        long time = pollAndMeasure(ensureConsumer, (getNumberOfTrueAlarms() * 2) + getNumberOfFalseAlarms() + getNumberOfUncaughtBreaches());
        ensureConsumer.close();

        CaseStudyBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of TOTAL ALARMS", "Total TIME [ns]"),
                Arrays.asList(Integer.toString((getNumberOfTrueAlarms() * 2) + getNumberOfFalseAlarms() + getNumberOfUncaughtBreaches()), Long.toString(time)));
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.print("--ERROR-- \nUSAGE: Ensure <topic> <totalNumberOfAlarms> <falseAlarmsPercentage> <alarmsNotTriggeredPercentage>");
            System.exit(0);
        }

        new Ensure(args[0], parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }
}
