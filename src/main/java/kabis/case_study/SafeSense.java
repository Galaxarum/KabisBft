package kabis.case_study;

import kabis.producer.KabisProducer;
import org.bouncycastle.jce.provider.BouncyCastleProvider;


import java.io.FileInputStream;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;

public class SafeSense extends ArtExhibitionProducer {
    public SafeSense(String topic, Integer totalNumberOfAlarms, Float falseAlarmsPercentage, Float alarmsNotTriggeredPercentage) {
        super(topic, totalNumberOfAlarms, falseAlarmsPercentage, alarmsNotTriggeredPercentage);
    }

    private void run() {
        Security.addProvider(new BouncyCastleProvider());
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        properties.setProperty("client.id", getTopic() + "-SafeSense");

        // Thread.sleep(10000);

        KabisProducer<Integer,String> safeSenseProducer = new KabisProducer<>(properties);
        safeSenseProducer.updateTopology(Collections.singletonList(getTopic()));
        System.out.printf("[%s-SafeSense] Kabis Producer created%n", getTopic());

        String message = "[ALARM] SafeSense - Location: ";

        // Thread.sleep(15000);
        int numberOfCaughtBreaches = Math.round((getTotalNumberOfAlarms() * (1 - getAlarmsNotTriggeredPercentage())));
        int numberOfFalseAlarms = Math.round((getTotalNumberOfAlarms() * getFalseAlarmsPercentage()));
        Integer numberOfAlarms = numberOfFalseAlarms + numberOfCaughtBreaches;

        long time = sendAndMeasure(safeSenseProducer, numberOfAlarms, message);

        safeSenseProducer.close();
        CaseStudyBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of caught breaches", "Number of false alarms", "Total time [ns]"),
                Arrays.asList(Integer.toString(numberOfCaughtBreaches), Integer.toString(numberOfFalseAlarms), Long.toString(time)));
    }

    public static void main(String[] args) {
        if(args.length != 4) {
            System.out.print("--ERROR-- \nUSAGE: SafeSense <topic> <totalNumberOfAlarms> <falseAlarmsPercentage> <alarmsNotTriggeredPercentage>");
            System.exit(0);
        }

        new SafeSense(args[0], parseInt(args[1]), parseFloat(args[2]), parseFloat(args[3])).run();
    }
}
