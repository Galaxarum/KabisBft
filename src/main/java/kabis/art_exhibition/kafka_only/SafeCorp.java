package kabis.art_exhibition.kafka_only;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;

public class SafeCorp extends ArtExhibitionProducer {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    protected SafeCorp(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    public static void main(String[] args) throws InterruptedException {
        // -- CHECK IF ALL ARGUMENTS ARE PRESENT --
        if (args.length != 5) {
            System.out.println("--ERROR-- \nUSAGE: SafeSense <clientId> <numberOfArtExhibitions> <numberOfTrueAlarms> <numberOfFalseAlarms> <numberOfUncaughtBreaches>");
            System.exit(1);
        }
        // -- PRIMING KAFKA, CREATING TOPICS --
        Thread.sleep(15000);
        // -- RUN SAFECORP INSTANCE --
        new SafeCorp(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3]), parseInt(args[4])).run();
    }

    private void run() {
        Properties consumerProperties = readProperties("consumer.config.properties");
        consumerProperties.setProperty("client.id", String.valueOf(getClientId()));

        Properties producerProperties = readProperties("producer.config.properties");
        producerProperties.setProperty("client.id", String.valueOf(getClientId()));

        KafkaConsumer<Integer, String> safeCorpConsumer = new KafkaConsumer<>(consumerProperties);
        safeCorpConsumer.subscribe(TOPICS);
        System.out.println("[SafeCorp - Kafka Only] Kabis Consumer created");

        KafkaProducer<Integer, String> safeCorpProducer = new KafkaProducer<>(producerProperties);
        System.out.println("[SafeCorp - Kafka Only] Kabis Producer created");

        List<TopicPartition> assignedPartitions = new ArrayList<>(safeCorpConsumer.assignment());
        List<Integer> assignedPartitionsIDs = assignedPartitions.stream().map(TopicPartition::partition).collect(Collectors.toList());

        // -- READ TRUE AND FALSE ALARMS AND RESPOND --
        System.out.println("[SafeCorp - Kafka Only] Reading alarms");
        String responseMessage = "[SafeCorp - Kafka Only] ALARM RECEIVED ";
        int numberOfAssignedPartitions = assignedPartitions.size();
        System.out.println("[SafeCorp - Kafka Only] Number of assigned exhibitions: " + numberOfAssignedPartitions);
        int recordsToRead = (getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * numberOfAssignedPartitions;
        LocalTime[] pollTimeResults = pollAndRespondMeasure(safeCorpConsumer, safeCorpProducer, recordsToRead, responseMessage);
        safeCorpConsumer.close();

        System.out.println("[SafeCorp - Kafka Only] READING DONE! Consumer Closed");

        // -- SEND UNCAUGHT ALARMS --
        System.out.println("[SafeCorp - Kafka Only] Sending uncaught breaches");
        String sendMessage = "[SafeCorp - Kafka Only] BREACH FOUND ";
        LocalTime[] pushTimeResults = sendAndMeasureCertainPartitions(safeCorpProducer, assignedPartitionsIDs, getNumberOfUncaughtBreaches(), sendMessage);

        LocalTime[] timeResults = new LocalTime[]{pollTimeResults[0], pushTimeResults[1]};
        safeCorpProducer.close();
        System.out.println("[SafeCorp - Kafka Only] DONE! Producer Closed - Saving experiments");

        // Store results
        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TRUE-ALARMS", "#UNCAUGHT-BREACHES", "START TIME", "END TIME"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(getNumberOfTrueAlarms()), Integer.toString(getNumberOfUncaughtBreaches()), timeResults[0].toString(), timeResults[1].toString()));
        System.out.println("[SafeCorp - Kafka Only] Experiments persisted!");
    }

    protected LocalTime[] pollAndRespondMeasure(KafkaConsumer<Integer, String> consumer, KafkaProducer<Integer, String> producer, Integer recordsToRead, String message) {
        int i = 0;
        LocalTime startTime = LocalTime.now();
        System.out.println("[pollAndRespondMeasure - Kafka Only]: recordsToRead: " + recordsToRead + " with POLL_TIMEOUT: " + POLL_TIMEOUT);
        while (i < recordsToRead) {
            ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<Integer, String> record : records) {
                String recordMessage = record.value();
                if (!recordMessage.contains("[SafeCorp - Kafka Only]")) {
                    i += 1;
                    ProducerRecord<Integer, String> responseRecord = new ProducerRecord<>(Topics.ART_EXHIBITION.toString(), record.partition(), record.key(), message + recordMessage);
                    System.out.println("[pollAndRespondMeasure - Kafka Only]: Sending " + responseRecord.value() + " exhibition: " + responseRecord.partition());
                    producer.send(responseRecord);
                }
            }
        }
        LocalTime endTime = LocalTime.now();
        System.out.println("[pollAndRespondMeasure - Kafka Only]: All messages read!");

        return new LocalTime[]{startTime, endTime};
    }

    protected LocalTime[] sendAndMeasureCertainPartitions(KafkaProducer<Integer, String> producer, List<Integer> partitions, Integer numberOfAlarms, String message) {
        LocalTime startTime = LocalTime.now();
        System.out.println("[sendAndMeasureCertainPartitions - Kafka Only]: numberOfArtExhibitions: " + partitions.size() + " numberOfAlarms:" + numberOfAlarms);
        partitions.forEach(partition -> {
            for (int i = 0; i < numberOfAlarms; i++) {
                ProducerRecord<Integer, String> record = new ProducerRecord<>(kabis.art_exhibition.Topics.ART_EXHIBITION.toString(), partition, partition, message + i);
                System.out.println("[sendAndMeasureCertainPartitions - Kafka Only]: Sending " + record.value() + " exhibition: " + record.partition());
                producer.send(record);
            }
        });
        LocalTime endTime = LocalTime.now();
        System.out.println("[sendAndMeasureCertainPartitions - Kafka Only]: All messages sent!");
        producer.flush();

        return new LocalTime[]{startTime, endTime};
    }
}