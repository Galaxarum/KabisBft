package kabis.benchmark.bft;

import kabis.benchmark.BenchmarkResult;
import kabis.validation.KabisServiceProxy;
import kabis.validation.SecureIdentifier;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;

import static kabis.benchmark.BenchmarkResult.TOPICS;

public class BftOnlyReceiver {
    private static final Logger LOG = LoggerFactory.getLogger(BftOnlySender.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 4) {
            LOG.error("USAGE: {} <id> <opsPerSender (multiple of {})> <numSenders>",
                    BftOnlySender.class.getCanonicalName(),
                    TOPICS.size()
            );
            System.exit(1);
        }
        Security.addProvider(new BouncyCastleProvider());
        SecureIdentifier.setUseSignatures(false);

        int clientId = Integer.parseInt(args[0]);
        int messagesPerSender = Integer.parseInt(args[1]);
        int numSenders = Integer.parseInt(args[2]);
        int payload = Integer.parseInt(args[3]);
        int totalMessages = messagesPerSender * numSenders;

        Thread.sleep(10000);

        KabisServiceProxy proxy = KabisServiceProxy.getInstance();
        proxy.init(clientId, false);

        //Real measure
        System.out.printf("BFT %d: Reading %d messages.%n", payload, totalMessages);
        var time = measureTotalConsumeTime(proxy, totalMessages);
        BenchmarkResult.storeThroughputToDisk(BenchmarkResult.buildThroughputString(totalMessages, payload, Integer.MAX_VALUE, time));
        System.out.println("Experiment result persisted");
        Thread.sleep(1000);
        System.exit(0);
    }

    public static long measureTotalConsumeTime(KabisServiceProxy proxy, int recordsToRead) {
        int i = 0;
        var t1 = System.nanoTime();
        while (i < recordsToRead) {
            // THIS WON'T WORK BECAUSE THE CLIENT MUST PASS A LIST OF TOPIC PARTITIONS IN NEW VERSIONS!
            i += proxy.pull(new ArrayList<>()).size();
        }
        var t2 = System.nanoTime();
        return t2 - t1;
    }

}

