package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;
import kabis.producer.KabisProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.security.Security;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class SafeCorp extends ArtExhibitionProducer {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    public SafeCorp(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    protected long pollAndRespondMeasure(KabisConsumer<Integer, String> consumer, KabisProducer<Integer, String> producer, Integer recordsToRead, String message) {
        int i = 0;

        long t1 = System.nanoTime();
        System.out.println("[pollAndRespondMeasure]: recordsToRead: " + recordsToRead);
        while (i < recordsToRead) {
            ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<Integer, String> record : records) {
                String recordMessage = record.value();
                if (!recordMessage.contains("[SafeCorp]")) {
                    i += 1;
                    System.out.println("[pollAndRespondMeasure]: Received " + recordMessage + "  exhibition: " + record.key());
                    ProducerRecord<Integer, String> responseRecord = new ProducerRecord<>(Topics.ART_EXHIBITION.toString(), record.key(), message);
                    System.out.println("Sending " + responseRecord.value() + " exhibition: " + responseRecord.key());
                    producer.push(responseRecord);
                }

            }
        }
        producer.flush();
        long t2 = System.nanoTime();
        System.out.println("[pollAndRespondMeasure]: All messages read!");

        return t2 - t1;
    }

    private void run() {
        Security.addProvider(new BouncyCastleProvider());
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        properties.setProperty("client.id", String.valueOf(getClientId()));

        KabisConsumer<Integer, String> safeCorpConsumer = new KabisConsumer<>(properties);
        safeCorpConsumer.subscribe(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        safeCorpConsumer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.println("[SafeCorp] Kabis Consumer created");

        KabisProducer<Integer, String> safeCorpProducer = new KabisProducer<>(properties);
        safeCorpProducer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.println("[SafeCorp] Kabis Producer created");

        // -- READ TRUE AND FALSE ALARMS AND RESPOND --
        System.out.println("[SafeCorp] Reading alarms");
        String responseMessage = "[SafeCorp] ALARM RECEIVED ";
        Integer recordsToRead = (getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * getNumberOfArtExhibitions();
        long receivingTime = pollAndRespondMeasure(safeCorpConsumer, safeCorpProducer, recordsToRead, responseMessage);
        safeCorpConsumer.close();

        System.out.println("[SafeCorp] READING DONE!");

        // -- SEND UNCAUGHT ALARMS --
        System.out.println("[SafeCorp] Sending uncaught breaches");
        String sendMessage = "[SafeCorp] BREACH FOUND";
        long sendingTime = sendAndMeasure(safeCorpProducer, getNumberOfUncaughtBreaches(), sendMessage);
        safeCorpProducer.close();

        System.out.println("[SafeCorp] DONE! Producer Closed - Saving experiments");

        // Store results
        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TRUE-ALARMS", "#UNCAUGHT-BREACHES", "TOTAL TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(getNumberOfTrueAlarms()), Integer.toString(getNumberOfUncaughtBreaches()), Long.toString(sendingTime + receivingTime)));
        System.out.println("[SafeCorp] Experiments persisted!");
    }

    public static void main(String[] args) throws InterruptedException {
        // -- CHECK IF ALL ARGUMENTS ARE PRESENT --
        if (args.length != 5) {
            System.out.println("--ERROR-- \nUSAGE: SafeSense <clientId> <numberOfArtExhibitions> <numberOfTrueAlarms> <numberOfFalseAlarms> <numberOfUncaughtBreaches>");
            System.exit(1);
        }
        new SafeCorp(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3]), parseInt(args[4])).run();
    }
}
