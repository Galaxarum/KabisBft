package kabis.benchmark;

import kabis.producer.KabisProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.Security;
import java.util.Properties;
import java.util.Random;

import static kabis.benchmark.BenchmarkResult.TOPICS;

public class KabisSender{
    private static final Logger LOG = LoggerFactory.getLogger(KabisSender.class);
    public static long measureSendingTime(KabisProducer<Integer,String> producer, int numRequests, String message){
        var t1 = System.nanoTime();
        for(int i=0;i<numRequests;i++){
            var iMod = i%TOPICS.size();
            var record = new ProducerRecord<>(TOPICS.get(iMod),0,i,message);
            producer.push(record);
        }
        producer.flush();
        var t2 = System.nanoTime();
        return t2-t1;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length!=4){
            LOG.error("USAGE: {} <id> <numOperations (multiple of {})> <payloadSize (in bytes)> <numValidatedTopics (max {})>",
                    KabisSender.class.getCanonicalName(),
                    TOPICS.size(),
                    TOPICS.size()
                    );
            System.exit(1);
        }

        Security.addProvider(new BouncyCastleProvider());
        int clientId = Integer.parseInt(args[0]);
        int numOperations = Integer.parseInt(args[1]);
        int payloadSize = Integer.parseInt(args[2]);
        int numValidatedTopics = Integer.parseInt(args[3]);
        byte[] sentBytes = new byte[payloadSize];
        new Random().nextBytes(sentBytes);
        String message = new String(sentBytes);

        Properties properties = new Properties();
        properties.load(new FileInputStream("config.properties"));
        properties.setProperty("client.id", String.valueOf(clientId));

        Thread.sleep(10000);

        KabisProducer<Integer,String> producer = new KabisProducer<>(properties);
        producer.updateTopology(TOPICS.subList(0,numValidatedTopics));

        //Prime kafka infrastructure
        System.out.printf("Sending %d messages to prime the system. [%d/%d] validated topics%n",TOPICS.size(),numValidatedTopics,TOPICS.size());
        measureSendingTime(producer,TOPICS.size(),message);
        Thread.sleep(1000);
        System.out.println("Kafka infrastructure primed");

        //Real measure
        System.out.printf("Sending %d messages. [%d/%d] validated topics%n",numOperations,numValidatedTopics,TOPICS.size());
        var time = measureSendingTime(producer,numOperations,message);
        BenchmarkResult.storeThroughputToDisk(BenchmarkResult.buildThroughputString(numOperations,payloadSize,numValidatedTopics,time));
        System.out.println("Experiment result persisted");
        producer.close();
    }

}

