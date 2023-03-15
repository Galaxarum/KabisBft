package kabis.benchmark.kafka;

import kabis.benchmark.BenchmarkResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import static kabis.benchmark.BenchmarkResult.TOPICS;

public class KafkaOnlySender {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOnlySender.class);

    public static long measureSendingTime(KafkaProducer<Integer,String> producer,int numRequests,String message){
        var t1 = System.nanoTime();
        for(int i=0;i<numRequests;i++){
            var iMod = i%TOPICS.size();
            var record = new ProducerRecord<>(TOPICS.get(iMod),0,i,message);
            producer.send(record);
        }
        producer.flush();
        var t2 = System.nanoTime();
        return t2-t1;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length!=3){
            LOG.error("USAGE: {} <id> <numOperations (multiple of {})> <payloadSize (in bytes)>",
                    KafkaOnlySender.class.getCanonicalName(),
                    TOPICS.size()
                    );
            System.exit(1);
        }

        int clientId = Integer.parseInt(args[0]);
        int numOperations = Integer.parseInt(args[1]);
        int payloadSize = Integer.parseInt(args[2]);
        byte[] sentBytes = new byte[payloadSize];
        new Random().nextBytes(sentBytes);
        String message = new String(sentBytes);

        Properties properties = new Properties();
        properties.load(new FileInputStream("config.properties"));
        properties.setProperty("client.id", String.valueOf(clientId));

        KafkaProducer<Integer,String> producer = new KafkaProducer<>(properties);

        //Prime kafka infrastructure
        measureSendingTime(producer,TOPICS.size(),message);
        Thread.sleep(15000);
        System.out.println("Kafka infrastructure primed");

        var time = measureSendingTime(producer,numOperations,message);
        BenchmarkResult.storeThroughputToDisk(BenchmarkResult.buildThroughputString(numOperations,payloadSize,0,time));
    }

}

