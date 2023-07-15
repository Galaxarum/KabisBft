package kabis.benchmark;

import kabis.consumer.KabisConsumer;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.Security;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static kabis.benchmark.BenchmarkResult.TOPICS;

public class KabisReceiver {
    private static final Logger LOG = LoggerFactory.getLogger(KabisSender.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 5) {
            LOG.error("USAGE: {} <id> <opsPerSender (multiple of {})> <numSenders> <numValidatedTopics (max {})> <payload>",
                    KabisSender.class.getCanonicalName(),
                    TOPICS.size(),
                    TOPICS.size()
            );
            LOG.error("PROVIDED ARGS: {}", Arrays.toString(args));
            System.exit(1);
        }

        Security.addProvider(new BouncyCastleProvider());
        int clientId = Integer.parseInt(args[0]);
        int messagesPerSender = Integer.parseInt(args[1]);
        int numSenders = Integer.parseInt(args[2]);
        int numValidatedTopics = Integer.parseInt(args[3]);
        int payload = Integer.parseInt(args[4]);
        int totalMessages = messagesPerSender * numSenders;

        Properties properties = new Properties();
        properties.load(new FileInputStream("config.properties"));
        properties.setProperty("client.id", String.valueOf(clientId));

        KabisConsumer<Integer, String> consumer = new KabisConsumer<>(properties);
        consumer.subscribe(TOPICS);
        consumer.updateTopology(TOPICS.subList(0, numValidatedTopics));

        //Prime kafka infrastructure
        var primingMessages = numSenders * TOPICS.size();
        System.out.printf("Reading %d messages to prime the system. [%d/%d] validated topics%n", primingMessages, numValidatedTopics, TOPICS.size());
        measureTotalConsumeTime(consumer, primingMessages);
        Thread.sleep(10000);
        System.out.println("Kafka infrastructure primed");

        //Real measure
        System.out.printf("KABIS %d: Reading %d messages. [%d/%d] validated topics%n", payload, totalMessages, numValidatedTopics, TOPICS.size());
        var time = measureTotalConsumeTime(consumer, totalMessages);
        BenchmarkResult.storeThroughputToDisk(BenchmarkResult.buildThroughputString(totalMessages, payload, numValidatedTopics, time));
        System.out.println("Experiment result persisted");
        consumer.close();
        Thread.sleep(1000);
        System.exit(0);
    }

    public static long measureTotalConsumeTime(KabisConsumer<Integer, String> consumer, int recordsToRead) {
        int i = 0;
        var t1 = System.nanoTime();
        while (i < recordsToRead) {
            i += consumer.poll(POLL_TIMEOUT).count();
        }
        var t2 = System.nanoTime();
        return t2 - t1;
    }

}

