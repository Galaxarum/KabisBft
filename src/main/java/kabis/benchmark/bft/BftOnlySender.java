package kabis.benchmark.bft;

import kabis.benchmark.BenchmarkResult;
import kabis.validation.KabisServiceProxy;
import kabis.validation.SecureIdentifier;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Security;
import java.util.Random;

import static kabis.benchmark.BenchmarkResult.TOPICS;

public class BftOnlySender {
    private static final Logger LOG = LoggerFactory.getLogger(BftOnlySender.class);

    public static long measureSendingTime(KabisServiceProxy serviceProxy,int numRequests,String message,int senderId){
        var t1 = System.nanoTime();
        for(int i=0;i<numRequests;i++){
            var iMod = i%TOPICS.size();
            var record = SecureIdentifier.factory(i,message,TOPICS.get(iMod),0,senderId);
            serviceProxy.push(record);
        }
        var t2 = System.nanoTime();
        return t2-t1;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length!=3){
            LOG.error("USAGE: {} <id> <numOperations (multiple of {})> <payloadSize (in bytes)>",
                    BftOnlySender.class.getCanonicalName(),
                    TOPICS.size()
                    );
            System.exit(1);
        }
        Security.addProvider(new BouncyCastleProvider());
        SecureIdentifier.setUseSignatures(false);

        int clientId = Integer.parseInt(args[0]);
        int numOperations = Integer.parseInt(args[1]);
        int payloadSize = Integer.parseInt(args[2]);
        byte[] sentBytes = new byte[payloadSize];
        new Random().nextBytes(sentBytes);
        String message = new String(sentBytes);

        Thread.sleep(30000);

        KabisServiceProxy proxy = new KabisServiceProxy(clientId);

        var time = measureSendingTime(proxy,numOperations,message,clientId);
        BenchmarkResult.storeThroughputToDisk(BenchmarkResult.buildThroughputString(numOperations,payloadSize,0,time));
        System.out.println("Experiment result saved");
    }

}

