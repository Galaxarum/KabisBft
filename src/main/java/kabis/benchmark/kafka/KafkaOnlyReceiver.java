package kabis.benchmark.kafka;

import kabis.benchmark.BenchmarkResult;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import static kabis.benchmark.BenchmarkResult.TOPICS;

public class KafkaOnlyReceiver {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOnlySender.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    public static long measureTotalConsumeTime(KafkaConsumer<Integer, String> consumer, int recordsToRead){
        int i=0;
        var t1 = System.nanoTime();
        while (i<recordsToRead){
            i += consumer.poll(POLL_TIMEOUT).count();
        }
        var t2 = System.nanoTime();
        return t2-t1;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length!=4){
            LOG.error("USAGE: {} <id> <opsPerSender (multiple of {})> <numSenders>",
                    KafkaOnlySender.class.getCanonicalName(),
                    TOPICS.size()
                    );
            System.exit(1);
        }

        int clientId = Integer.parseInt(args[0]);
        int messagesPerSender = Integer.parseInt(args[1]);
        int numSenders = Integer.parseInt(args[2]);
        int payload = Integer.parseInt(args[3]);
        int totalMessages = messagesPerSender * numSenders;

        Properties properties = new Properties();
        properties.load(new FileInputStream("config.properties"));
        properties.setProperty("client.id", String.format("%d-consumer-1", clientId));
        KafkaConsumer<Integer,String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(TOPICS);

        //Prime kafka infrastructure
        measureTotalConsumeTime(consumer,numSenders*TOPICS.size());
        Thread.sleep(30000);
        System.out.println("Kafka infrastructure primed");

        var time = measureTotalConsumeTime(consumer,totalMessages);

        BenchmarkResult.storeThroughputToDisk(BenchmarkResult.buildThroughputString(totalMessages,payload,-1,time));
        System.out.println("Experiment result persisted");
        consumer.close();
        Thread.sleep(1000);
        System.exit(0);
    }

}

