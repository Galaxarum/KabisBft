package kabis.validation;

import bftsmart.tom.MessageContext;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import org.apache.kafka.common.errors.SerializationException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class KabisServiceReplica extends DefaultSingleRecoverable {

    private static final Logger LOG = LoggerFactory.getLogger(KabisServiceReplica.class);
    private final List<SecureIdentifier> log = new LinkedList<>();

    @Override
    public void installSnapshot(byte[] bytes) {
        log.clear();
        log.addAll(deserializeSidList(bytes));
    }

    @Override
    public byte[] getSnapshot() {
        return pull(0);
    }

    @Override
    public byte[] appExecuteOrdered(byte[] bytes, MessageContext messageContext) {
        try (var cmd = new ByteArrayInputStream(bytes)) {
            var opOrdinal = cmd.read();
            var op = OPS.values()[opOrdinal];
            switch (op) {
                case PUSH:
                    push(cmd.readAllBytes());
                    return new byte[0];
                case PULL:
                    var index = ByteBuffer.wrap(cmd.readNBytes(Integer.BYTES)).getInt();
                    return pull(index);
                default:
                    throw new IllegalArgumentException(String.format("Illegal ordered operation requested: %s", op));
            }
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] appExecuteUnordered(byte[] bytes, MessageContext messageContext) {
        try (var cmd = new ByteArrayInputStream(bytes)) {
            var opOrdinal = cmd.read();
            if (opOrdinal == OPS.PULL.ordinal()) {
                var index = ByteBuffer.wrap(cmd.readNBytes(Integer.BYTES)).getInt();
                return pull(index);
            }
            throw new IllegalArgumentException(String.format("Illegal ordered operation requested: %s", OPS.values()[opOrdinal]));
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    public KabisServiceReplica(int id) {
        new bftsmart.tom.ServiceReplica(id, this, this);
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            LOG.error("USAGE: {} <process id>", KabisServiceReplica.class.getCanonicalName());
            System.exit(-1);
        }
        Security.addProvider(new BouncyCastleProvider());
        int processId = Integer.parseInt(args[0]);
        new KabisServiceReplica(processId);
    }

    private void push(byte[] serializedSid) {
        var sid = SecureIdentifier.deserialize(serializedSid);
        synchronized (log) {
            log.add(sid);
        }
    }

    private byte[] pull(int index) {
        if (index > log.size()) return new byte[0];
        List<SecureIdentifier> logPortion;
        synchronized (log) {
            logPortion = new ArrayList<>(log.subList(index, log.size()));
        }
        return serializeSidList(logPortion);
    }

    public static byte[] serializeSidList(List<SecureIdentifier> subLog) {
        try (var bytes = new ByteArrayOutputStream()) {
            for (var sid : subLog) {
                var serialized = sid.serialize();
                bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(serialized.length).array());
                bytes.writeBytes(serialized);
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<SecureIdentifier> deserializeSidList(byte[] serialized) {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized)) {
            List<SecureIdentifier> res = new LinkedList<>();
            while (bytes.available() > 0) {
                int len = ByteBuffer.wrap(bytes.readNBytes(Integer.BYTES)).getInt();
                byte[] serializedSid = bytes.readNBytes(len);
                res.add(SecureIdentifier.deserialize(serializedSid));
            }
            return res;
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

}