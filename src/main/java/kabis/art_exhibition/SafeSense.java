package kabis.art_exhibition;

import kabis.producer.KabisProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.lang.Integer.parseInt;

public class SafeSense extends ArtExhibitionProducer {

    protected SafeSense(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms) {
        super(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms);
    }

    public static void main(String[] args) throws InterruptedException {
        // -- CHECK IF ALL ARGUMENTS ARE PRESENT --
        if (args.length != 4) {
            System.out.print("--ERROR-- \nUSAGE: <clientId> <numberOfArtExhibitions> <numberOfTrueAlarms> <numberOfFalseAlarms>");
            System.exit(1);
        }
        new SafeSense(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }

    private void run() throws InterruptedException {
        createTopic("kafka_1_1:9092,kafka_1_2:9092,kafka_1_3:9092,kafka_1_4:9092,kafka_2_1:9092,kafka_2_2:9092,kafka_2_3:9092,kafka_2_4:9092");
        Thread.sleep(15000);

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

    private void createTopic(String kafkaBroker) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaBroker);
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        properties.put("delete.topic.enable", true);
        try (AdminClient client = AdminClient.create(properties)) {
            System.out.println("[SafeSense] AdminClient created!");
            DescribeTopicsResult checkTopicsResult = client.describeTopics(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
            try {
                Map<String, TopicDescription> topics = checkTopicsResult.allTopicNames().get();
                System.out.println("[SafeSense] " + kafkaBroker + " TOPICS STATUS: " + topics);
                while (topics.containsKey(Topics.ART_EXHIBITION.toString())) {
                    System.out.println("[SafeSense] Topic already exists for " + kafkaBroker + "!");
                    System.out.println("[SafeSense] Deleting topic for " + kafkaBroker + "...");
                    client.deleteTopics(Collections.singletonList(Topics.ART_EXHIBITION.toString())).all().get();
                    System.out.println("[SafeSense] Topic deleted successfully for " + kafkaBroker + "!");
                    topics = checkTopicsResult.allTopicNames().get();
                    System.out.println("[SafeSense] " + kafkaBroker + " TOPICS STATUS AFTER DELETE: " + topics);
                }
                System.out.println("[SafeSense] Creating topic for " + kafkaBroker + "...");
                client.createTopics(List.of(
                        new NewTopic(Topics.ART_EXHIBITION.toString(), getNumberOfArtExhibitions(), (short) 1)
                )).all().get();
                System.out.println("[SafeSense] Topic created successfully for " + kafkaBroker + "!");
                System.out.println("[SafeSense] Describe topic for " + kafkaBroker + ": " + checkTopicsResult.allTopicNames().get());
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
