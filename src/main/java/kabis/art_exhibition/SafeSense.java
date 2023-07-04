package kabis.art_exhibition;

import kabis.producer.KabisProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.lang.Integer.parseInt;

public class SafeSense extends ArtExhibitionProducer {

    protected SafeSense(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms) {
        super(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms);
    }

    private void run() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", getProperties().getProperty("bootstrap.servers"));
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        try (AdminClient client = AdminClient.create(properties)) {
            System.out.println("[SafeSense] Creating topic...");
            CreateTopicsResult result = client.createTopics(List.of(
                    new NewTopic(Topics.ART_EXHIBITION.toString(), getNumberOfArtExhibitions(), (short) 2)
            ));
            try {
                result.all().get();
                System.out.println("[SafeSense] Topic created successfully!");
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }
        
        KabisProducer<Integer, String> safeSenseProducer = new KabisProducer<>(getProperties());
        safeSenseProducer.updateTopology(TOPICS);
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
