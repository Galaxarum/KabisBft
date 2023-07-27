package kabis.art_exhibition.kafka_only;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

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

        // -- READ TRUE AND FALSE ALARMS AND RESPOND --
        System.out.println("[SafeCorp - Kafka Only] Reading alarms");
        String responseMessage = "[SafeCorp - Kafka Only] ALARM RECEIVED ";
        int numberOfAssignedPartitions = safeCorpConsumer.assignment().size();
        System.out.println("[SafeCorp - Kafka Only] Number of assigned exhibitions: " + numberOfAssignedPartitions);
        int recordsToRead = (getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * numberOfAssignedPartitions;
        long receivingTime = pollAndRespondMeasure(safeCorpConsumer, safeCorpProducer, recordsToRead, responseMessage);
        safeCorpConsumer.close();

        System.out.println("[SafeCorp - Kafka Only] READING DONE! Consumer Closed");

        // -- SEND UNCAUGHT ALARMS --
        System.out.println("[SafeCorp - Kafka Only] Sending uncaught breaches");
        String sendMessage = "[SafeCorp - Kafka Only] BREACH FOUND ";
        long sendingTime = sendAndMeasure(safeCorpProducer, getNumberOfUncaughtBreaches(), sendMessage);

        long totalTime = sendingTime + receivingTime;
        safeCorpProducer.close();
        System.out.println("[SafeCorp - Kafka Only] DONE! Producer Closed - Saving experiments");

        // Store results
        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TRUE-ALARMS", "#UNCAUGHT-BREACHES", "TOTAL TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(getNumberOfTrueAlarms()), Integer.toString(getNumberOfUncaughtBreaches()), Long.toString(totalTime)));
        System.out.println("[SafeCorp - Kafka Only] Experiments persisted!");
    }

    protected long pollAndRespondMeasure(KafkaConsumer<Integer, String> consumer, KafkaProducer<Integer, String> producer, Integer recordsToRead, String message) {
        int i = 0;
        long t1 = System.nanoTime();
        System.out.println("[pollAndRespondMeasure - Kafka Only]: recordsToRead: " + recordsToRead + " with POLL_TIMEOUT: " + POLL_TIMEOUT);
        while (i < recordsToRead) {
            ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
            System.out.println("[pollAndRespondMeasure - Kafka Only]: Received " + records.count() + " records");
            for (ConsumerRecord<Integer, String> record : records) {
                String recordMessage = record.value();
                if (!recordMessage.contains("[SafeCorp - Kafka Only]")) {
                    i += 1;
                    ProducerRecord<Integer, String> responseRecord = new ProducerRecord<>(Topics.ART_EXHIBITION.toString(), record.partition(), record.key(), message + recordMessage);
                    System.out.println("[pollAndRespondMeasure - Kafka Only]: Sending " + responseRecord.value() + " exhibition: " + responseRecord.key());
                    producer.send(responseRecord);
                }
            }
        }
        long t2 = System.nanoTime();
        System.out.println("[pollAndRespondMeasure - Kafka Only]: All messages read!");

        return t2 - t1;
    }
}
