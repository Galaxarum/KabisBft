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
    private final List<SecureIdentifier> secureIdentifierList = new LinkedList<>();

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

    @Override
    public void installSnapshot(byte[] bytes) {
        this.secureIdentifierList.clear();
        this.secureIdentifierList.addAll(deserializeSidList(bytes));
    }

    /**
     * This method is called when a snapshot is requested.
     * It returns the list of SecureIdentifiers as a byte array.
     *
     * @return a byte array containing the list of SecureIdentifiers
     */
    @Override
    public byte[] getSnapshot() {
        return pull(0);
    }

    /**
     * This method is called when an ordered request is received.
     *
     * @param bytes          the request as a byte array
     * @param messageContext the message context
     * @return the response as a byte array
     */
    @Override
    public byte[] appExecuteOrdered(byte[] bytes, MessageContext messageContext) {
        try (ByteArrayInputStream cmd = new ByteArrayInputStream(bytes)) {
            int opOrdinal = cmd.read();
            OPS op = OPS.values()[opOrdinal];
            switch (op) {
                case PUSH:
                    push(cmd.readAllBytes());
                    return new byte[0];
                case PULL:
                    int index = ByteBuffer.wrap(cmd.readNBytes(Integer.BYTES)).getInt();
                    return pull(index);
                default:
                    throw new IllegalArgumentException(String.format("Illegal ordered operation requested: %s", op));
            }
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * This method is called when an unordered request is received.
     *
     * @param bytes          the request as a byte array
     * @param messageContext the message context
     * @return the response as a byte array
     */
    @Override
    public byte[] appExecuteUnordered(byte[] bytes, MessageContext messageContext) {
        try (ByteArrayInputStream cmd = new ByteArrayInputStream(bytes)) {
            int opOrdinal = cmd.read();
            if (opOrdinal == OPS.PULL.ordinal()) {
                int index = ByteBuffer.wrap(cmd.readNBytes(Integer.BYTES)).getInt();
                return pull(index);
            }
            throw new IllegalArgumentException(String.format("Illegal ordered operation requested: %s", OPS.values()[opOrdinal]));
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Pushes a new SecureIdentifier to the list of SecureIdentifiers.
     *
     * @param serializedSid the serialized SecureIdentifier to push.
     */
    private void push(byte[] serializedSid) {
        SecureIdentifier sid = SecureIdentifier.deserialize(serializedSid);
        synchronized (this.secureIdentifierList) {
            this.secureIdentifierList.add(sid);
        }
    }

    /**
     * Pulls a portion of the list of SecureIdentifiers.
     *
     * @param index the index of the first SecureIdentifier to pull.
     * @return the serialized list of SecureIdentifiers.
     */
    private byte[] pull(int index) {
        if (index > this.secureIdentifierList.size()) return new byte[0];
        List<SecureIdentifier> secureIdentifierSubList;
        synchronized (this.secureIdentifierList) {
            secureIdentifierSubList = new ArrayList<>(this.secureIdentifierList.subList(index, this.secureIdentifierList.size()));
        }
        return serializeSidList(secureIdentifierSubList);
    }

    /**
     * Serializes a list of SecureIdentifiers to a byte array.
     *
     * @param sidList the list of SecureIdentifiers to serialize.
     * @return the serialized list of SecureIdentifiers.
     */
    public static byte[] serializeSidList(List<SecureIdentifier> sidList) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            for (SecureIdentifier sid : sidList) {
                byte[] serialized = sid.serialize();
                bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(serialized.length).array());
                bytes.writeBytes(serialized);
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserializes a list of SecureIdentifiers from a byte array.
     * The byte array is expected to be in the format produced by {@link #serializeSidList(List)}.
     *
     * @param serialized the byte array to deserialize.
     * @return the deserialized list of SecureIdentifiers.
     */
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