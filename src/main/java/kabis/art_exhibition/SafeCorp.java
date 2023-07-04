package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;
import kabis.producer.KabisProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class SafeCorp extends ArtExhibitionProducer {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    protected SafeCorp(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    public static void main(String[] args) throws InterruptedException {
        // -- CHECK IF ALL ARGUMENTS ARE PRESENT --
        if (args.length != 5) {
            System.out.println("--ERROR-- \nUSAGE: SafeSense <clientId> <numberOfArtExhibitions> <numberOfTrueAlarms> <numberOfFalseAlarms> <numberOfUncaughtBreaches>");
            System.exit(1);
        }
        // -- RUN SAFECORP INSTANCE --
        new SafeCorp(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3]), parseInt(args[4])).run();
        // -- KILL THE BENCHMARK AFTER run() --
        Thread.sleep(60000);
        System.exit(0);
    }

    private void run() throws InterruptedException {
        Thread.sleep(60000);
        Properties properties = getProperties();

        KabisConsumer<Integer, String> safeCorpConsumer = new KabisConsumer<>(properties);
        safeCorpConsumer.subscribe(TOPICS);
        safeCorpConsumer.updateTopology(TOPICS);
        System.out.println("[SafeCorp] Kabis Consumer created");

        KabisProducer<Integer, String> safeCorpProducer = new KabisProducer<>(properties);
        safeCorpProducer.updateTopology(TOPICS);
        System.out.println("[SafeCorp] Kabis Producer created");

        // -- READ TRUE AND FALSE ALARMS AND RESPOND --
        System.out.println("[SafeCorp] Reading alarms");
        String responseMessage = "[SafeCorp] ALARM RECEIVED ";
        // * getNumberOfArtExhibitions() will be removed when scaling on multiple consumers within the same consumer group,
        // every consumer will only read its own exhibition
        Integer recordsToRead = (getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * getNumberOfArtExhibitions();
        long receivingTime = pollAndRespondMeasure(safeCorpConsumer, safeCorpProducer, recordsToRead, responseMessage);
        safeCorpConsumer.close();

        System.out.println("[SafeCorp] READING DONE! Consumer Closed");

        // -- SEND UNCAUGHT ALARMS --
        System.out.println("[SafeCorp] Sending uncaught breaches");
        String sendMessage = "[SafeCorp] BREACH FOUND ";
        long sendingTime = sendAndMeasure(safeCorpProducer, getNumberOfUncaughtBreaches(), sendMessage);

        long totalTime = sendingTime + receivingTime;
        safeCorpProducer.close();
        System.out.println("[SafeCorp] DONE! Producer Closed - Saving experiments");

        // Store results
        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TRUE-ALARMS", "#UNCAUGHT-BREACHES", "TOTAL TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(getNumberOfTrueAlarms()), Integer.toString(getNumberOfUncaughtBreaches()), Long.toString(totalTime)));
        System.out.println("[SafeCorp] Experiments persisted!");
    }

    protected long pollAndRespondMeasure(KabisConsumer<Integer, String> consumer, KabisProducer<Integer, String> producer, Integer recordsToRead, String message) {
        int i = 0;
        long t1 = System.nanoTime();
        System.out.println("[pollAndRespondMeasure]: recordsToRead: " + recordsToRead + " with POLL_TIMEOUT: " + POLL_TIMEOUT);
        while (i < recordsToRead) {
            ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<Integer, String> record : records) {
                String recordMessage = record.value();
                if (!Arrays.equals(record.headers().lastHeader("clientId").value(), String.valueOf(getClientId()).getBytes())) {
                    i += 1;
                    System.out.println("[pollAndRespondMeasure]: Received " + recordMessage + " exhibition: " + record.partition());
                    ProducerRecord<Integer, String> responseRecord = new ProducerRecord<>(Topics.ART_EXHIBITION.toString(), record.key(), message + recordMessage);
                    responseRecord.headers().add("clientId", String.valueOf(getClientId()).getBytes());
                    System.out.println("[pollAndRespondMeasure]: Sending " + responseRecord.value() + " exhibition: " + responseRecord.key());
                    producer.push(responseRecord);
                    System.out.println("[pollAndRespondMeasure]: Message sent, waiting for next message");
                }
            }
        }
        producer.flush();
        long t2 = System.nanoTime();
        System.out.println("[pollAndRespondMeasure]: All messages read!");

        return t2 - t1;
    }
}
