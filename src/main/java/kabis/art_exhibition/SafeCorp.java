package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;
import kabis.producer.KabisProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class SafeCorp extends ArtExhibitionProducer {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    public SafeCorp(Integer artExhibitionID, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(artExhibitionID, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    protected long pollAndRespondMeasure(KabisConsumer<Integer, String> consumer, KabisProducer<Integer, String> producer, Integer recordsToRead, String message) {
        int i = 0;

        long t1 = System.nanoTime();
        while (i < recordsToRead) {
            ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<Integer, String> record : records) {
                if (!Arrays.equals(record.headers().lastHeader("sender").value(), this.getClass().toString().getBytes(StandardCharsets.UTF_8))) {
                    i += 1;

                    ProducerRecord<Integer, String> responseRecord = new ProducerRecord<>(Topics.ART_EXHIBITION.toString(), getArtExhibitionID(), message);
                    responseRecord.headers().add("sender", this.getClass().toString().getBytes(StandardCharsets.UTF_8));
                    producer.push(responseRecord);
                }

            }

        }
        producer.flush();
        long t2 = System.nanoTime();

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
        properties.setProperty("client.id", getArtExhibitionID() + "-SafeCorp");

        // Thread.sleep(10000);

        KabisConsumer<Integer, String> safeCorpConsumer = new KabisConsumer<>(properties);
        safeCorpConsumer.subscribe(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        safeCorpConsumer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.printf("[%s-SafeCorp] Kabis Consumer created\n", getArtExhibitionID());

        KabisProducer<Integer, String> safeCorpProducer = new KabisProducer<>(properties);
        safeCorpProducer.updateTopology(Collections.singletonList(Topics.ART_EXHIBITION.toString()));
        System.out.printf("[%s-SafeCorp] Kabis Producer created\n", getArtExhibitionID());

        // Thread.sleep(15000);

        // Read messages
        System.out.printf("[%s-SafeCorp] Reading alarms\n", getArtExhibitionID());
        String responseMessage = "[SafeCorp] TRUE ALARM RECEIVED";
        long receivingTime = pollAndRespondMeasure(safeCorpConsumer, safeCorpProducer, getNumberOfTrueAlarms() + getNumberOfFalseAlarms(), responseMessage);
        safeCorpConsumer.close();

        // Send uncaught
        System.out.printf("[%s-SafeCorp] Sending uncaught breaches\n", getArtExhibitionID());
        String sendMessage = "[SafeCorp] BREACH FOUND";
        long sendingTime = sendAndMeasure(safeCorpProducer, getNumberOfUncaughtBreaches(), sendMessage);
        safeCorpProducer.close();

        // Store results
        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of TRUE ALARMS", "Number of UNCAUGHT BREACHES", "Total TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfTrueAlarms()), Integer.toString(getNumberOfUncaughtBreaches()), Long.toString(sendingTime + receivingTime)));
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("--ERROR-- \nUSAGE: SafeSense <artExhibitionID> <numberOfTrueAlarms> <numberOfFalseAlarms> <numberOfUncaughtBreaches>");
            System.exit(0);
        }

        new SafeCorp(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }
}