package kabis.art_exhibition;

import kabis.producer.KabisProducer;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class SafeSense extends ArtExhibitionProducer {

    public SafeSense(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms) {
        super(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms);
    }

    private void run() throws InterruptedException {
        Security.addProvider(new BouncyCastleProvider());
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        properties.setProperty("client.id", String.valueOf(getClientId()));

        Thread.sleep(10000);

        KabisProducer<Integer, String> safeSenseProducer = new KabisProducer<>(properties);
        safeSenseProducer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.printf("[SafeSense] Kabis Producer created\n");

        // -- SEND TRUE ALARMS --
        System.out.printf("[SafeSense] Sending TRUE ALARMS\n");
        String trueAlarmMessage = "[SafeSense] TRUE ALARM";
        long trueAlarmsTime = sendAndMeasure(safeSenseProducer, getNumberOfTrueAlarms(), trueAlarmMessage);

        // -- SEND FALSE ALARMS --
        System.out.printf("[SafeSense] Sending FALSE ALARMS\n");
        String falseAlarmMessage = "[SafeSense] FALSE ALARM";
        long falseAlarmTime = sendAndMeasure(safeSenseProducer, getNumberOfFalseAlarms(), falseAlarmMessage);
        safeSenseProducer.close();

        long time = trueAlarmsTime + falseAlarmTime;

        System.out.printf("[SafeSense] DONE! Producer Closed - Saving experiments\n");

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of TRUE ALARMS", "Number of FALSE ALARMS", "Total TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfTrueAlarms()), Integer.toString(getNumberOfFalseAlarms()), Long.toString(time)));
        System.out.printf("[SafeSense] Experiments persisted!\n");
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 4) {
            System.out.print("--ERROR-- \nUSAGE: <clientId> <numberOfArtExhibitions> <numberOfTrueAlarms> <numberOfFalseAlarms>");
            System.exit(0);
        }

        new SafeSense(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }
}
