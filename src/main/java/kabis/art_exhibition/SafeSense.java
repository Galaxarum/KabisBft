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

        KabisProducer<Integer, String> safeSenseProducer = new KabisProducer<>(properties);
        safeSenseProducer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.println("[SafeSense] Kabis Producer created");

        // -- SEND TRUE ALARMS --
        System.out.println("[SafeSense] Sending TRUE ALARMS");
        String trueAlarmMessage = "[SafeSense] TRUE ALARM ";
        long trueAlarmsTime = sendAndMeasure(safeSenseProducer, getNumberOfTrueAlarms(), trueAlarmMessage);

        // -- SEND FALSE ALARMS --
        System.out.println("[SafeSense] Sending FALSE ALARMS");
        String falseAlarmMessage = "[SafeSense] FALSE ALARM ";
        long falseAlarmTime = sendAndMeasure(safeSenseProducer, getNumberOfFalseAlarms(), falseAlarmMessage);

        long totalTime = trueAlarmsTime + falseAlarmTime;
        safeSenseProducer.close();
        System.out.println("[SafeSense] DONE! Producer Closed - Saving experiments");

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TRUE-ALARMS", "#FALSE-ALARMS", "TOTAL TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(getNumberOfTrueAlarms()), Integer.toString(getNumberOfFalseAlarms()), Long.toString(totalTime)));
        System.out.println("[SafeSense] Experiments persisted!");
    }

    public static void main(String[] args) throws InterruptedException {
        // -- CHECK IF ALL ARGUMENTS ARE PRESENT --
        if (args.length != 4) {
            System.out.print("--ERROR-- \nUSAGE: <clientId> <numberOfArtExhibitions> <numberOfTrueAlarms> <numberOfFalseAlarms>");
            System.exit(1);
        }
        new SafeSense(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }
}
