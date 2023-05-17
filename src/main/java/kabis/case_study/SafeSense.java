package kabis.case_study;

import kabis.benchmark.KabisSender;
import kabis.producer.KabisProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class SafeSense {
    private static final Logger LOG = LoggerFactory.getLogger(KabisSender.class);
    private final String topic;
    private final Integer numberOfSecurityBreaches;
    private final Integer numberOfFalseAlarms;

    public SafeSense(String topic, Integer numberOfSecurityBreaches, Integer numberOfFalseAlarms) {
        this.topic = topic;
        this.numberOfSecurityBreaches = numberOfSecurityBreaches;
        this.numberOfFalseAlarms = numberOfFalseAlarms;
    }

    public SafeSense(String topic, Integer numberOfSecurityBreaches) {
        this(topic, numberOfSecurityBreaches, 0);
    }

    private long sendAndMeasure(KabisProducer<Integer,String> producer, String message){
        long t1 = System.nanoTime();

        for(int i = 0; i < this.numberOfSecurityBreaches; i++) {
            var record = new ProducerRecord<>(this.topic, parseInt(this.topic), message + i);
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
        properties.setProperty("client.id", this.topic + "-SafeSense");

        // Thread.sleep(10000);

        KabisProducer<Integer,String> safeSenseProducer = new KabisProducer<>(properties);
        safeSenseProducer.updateTopology(Collections.singletonList(this.topic));
        System.out.printf("[%s-SafeSense] Kabis Producer created%n", this.topic);

        String message = "[ALARM] SafeSense - Location: ";

        // Thread.sleep(15000);

        long time = sendAndMeasure(safeSenseProducer, message);
        safeSenseProducer.close();
        CaseStudyBenchmarkResult.storeThroughputToDisk(Arrays.asList("Number of security breaches", "Number of false alarms", "Total time [ns]"),
                Arrays.asList(Integer.toString(this.numberOfSecurityBreaches), Integer.toString(this.numberOfFalseAlarms), Long.toString(time)));
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            new SafeSense(args[0], parseInt(args[1])).run();
        }
        if (args.length == 3 && (args[2].equalsIgnoreCase("true") || args[2].equalsIgnoreCase("false"))) {
            new SafeSense(args[0], parseInt(args[1]), parseInt(args[2])).run();
        }

        LOG.error("USAGE: SafeSense <topic> <numberOfSecurityBreaches> <numberOfFalseAlarms (optional, default 0)>");
    }
}
