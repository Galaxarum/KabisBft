package kabis.art_exhibition;

import kabis.producer.KabisProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class SafeSense extends ArtExhibitionProducer {

    public SafeSense(String topic, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(topic, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    protected long sendAndMeasure(KabisProducer<Integer, String> producer, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms) {
        long t1 = System.nanoTime();

        for (int i = 0; i < numberOfTrueAlarms; i++) {
            String trueAlarmMessage = "[SafeSense] TRUE ALARM";
            ProducerRecord<Integer, String> record = new ProducerRecord<>(getTopic(), parseInt(getTopic()), trueAlarmMessage);
            record.headers().add("sender", this.getClass().toString().getBytes(StandardCharsets.UTF_8));
            producer.push(record);
        }
        for (int i = 0; i < numberOfFalseAlarms; i++) {
            String falseAlarmMessage = "- [SafeSense] FALSE ALARM - ";
            ProducerRecord<Integer, String> record = new ProducerRecord<>(getTopic(), parseInt(getTopic()), falseAlarmMessage);
            producer.push(record);
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
        properties.setProperty("client.id", getTopic() + "-SafeSense");

        // Thread.sleep(10000);

        KabisProducer<Integer, String> safeSenseProducer = new KabisProducer<>(properties);
        safeSenseProducer.updateTopology(Collections.singletonList(getTopic()));
        System.out.printf("[%s-SafeSense] Kabis Producer created\n", getTopic());

        // Send true alarms
        System.out.printf("[%s-SafeSense] Sending ALARMS\n", getTopic());
        long time = sendAndMeasure(safeSenseProducer, getNumberOfTrueAlarms(), getNumberOfFalseAlarms());
        safeSenseProducer.close();

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of TRUE ALARMS", "Number of FALSE ALARMS", "Total TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfTrueAlarms()), Integer.toString(getNumberOfFalseAlarms()), Long.toString(time)));
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.print("--ERROR-- \nUSAGE: SafeSense <topic> <numberOfTrueAlarms> <numberOfFalseAlarms> <numberOfUncaughtBreaches>");
            System.exit(0);
        }

        new SafeSense(args[0], parseInt(args[1]), parseInt(args[2]), parseInt(args[3])).run();
    }
}
