package kabis.case_study;

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

    public SafeCorp(String topic, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(topic, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    protected long pollAndRespondMeasure(KabisConsumer<Integer, String> consumer, KabisProducer<Integer, String> producer, Integer recordsToRead, String message) {
        int i = 0;

        long t1 = System.nanoTime();
        while (i < recordsToRead) {
            ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<Integer, String> record : records) {
                if (!Arrays.equals(record.headers().lastHeader("sender").value(), this.getClass().toString().getBytes(StandardCharsets.UTF_8))) {
                    i += 1;

                    ProducerRecord<Integer, String> responseRecord = new ProducerRecord<>(getTopic(), parseInt(getTopic()), message);
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
        properties.setProperty("client.id", getTopic() + "-SafeCorp");

        // Thread.sleep(10000);

        KabisConsumer<Integer, String> safeCorpConsumer = new KabisConsumer<>(properties);
        safeCorpConsumer.subscribe(Collections.singletonList(getTopic()));
        safeCorpConsumer.updateTopology(Collections.singletonList(getTopic()));
        System.out.printf("[%s-SafeCorp] Kabis Consumer created\n", getTopic());

        KabisProducer<Integer, String> safeCorpProducer = new KabisProducer<>(properties);
        safeCorpProducer.updateTopology(Collections.singletonList(getTopic()));
        System.out.printf("[%s-SafeCorp] Kabis Producer created\n", getTopic());

        // Thread.sleep(15000);

        // Read messages
        System.out.printf("[%s-SafeCorp] Reading alarms\n", getTopic());
        String responseMessage = "[SafeCorp] TRUE ALARM RECEIVED";
        long receivingTime = pollAndRespondMeasure(safeCorpConsumer, safeCorpProducer, getNumberOfTrueAlarms(), responseMessage);
        safeCorpConsumer.close();

        // Send uncaught
        System.out.printf("[%s-SafeCorp] Sending breaches\n", getTopic());
        String sendMessage = "[SafeCorp] BREACH FOUND";
        long sendingTime = sendAndMeasure(safeCorpProducer, getNumberOfUncaughtBreaches(), sendMessage);
        safeCorpProducer.close();

        // Store results
        CaseStudyBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of TRUE ALARMS", "Number of UNCAUGHT BREACHES", "Total TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfTrueAlarms()), Integer.toString(getNumberOfUncaughtBreaches()), Long.toString(sendingTime + receivingTime)));
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("--ERROR-- \nUSAGE: SafeSense <topic> <numberOfTrueAlarms> <numberOfFalseAlarms> <numberOfUncaughtBreaches>");
            System.exit(0);
        }

        new SafeCorp(args[0], parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }
}
