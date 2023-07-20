package kabis.validation;

import bftsmart.tom.MessageContext;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import kabis.validation.serializers.ServiceReplicaResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static kabis.validation.serializers.ServiceReplicaResponse.*;
import static kabis.validation.serializers.TopicPartitionListSerializer.deserializeTopicPartitionList;

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
        return serializeSidList(this.secureIdentifierList);
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
                    List<TopicPartition> topicPartitions = deserializeTopicPartitionList(cmd.readAllBytes());
                    return pull(index, topicPartitions);
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
                List<TopicPartition> topicPartitions = deserializeTopicPartitionList(cmd.readAllBytes());
                return pull(index, topicPartitions);
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
     * Pulls a portion of the list of SecureIdentifiers. Filtered by the given TopicPartitions.
     *
     * @param index           the index to start pulling from.
     * @param topicPartitions the list of TopicPartitions to pull.
     * @return the serialized list of SecureIdentifiers.
     */
    private byte[] pull(int index, List<TopicPartition> topicPartitions) {
        if (index > this.secureIdentifierList.size()) return new byte[0];
        //TODO: Remove print
        System.out.println("Pulling with topic partitions " + topicPartitions + " and index " + index);
        List<SecureIdentifier> secureIdentifierSubList;

        synchronized (this.secureIdentifierList) {
            secureIdentifierSubList = this.secureIdentifierList
                    .subList(index, this.secureIdentifierList.size())
                    .stream()
                    .filter(sid -> topicPartitions.contains(sid.getTopicPartition()))
                    .collect(Collectors.toList());
        }
        return serializeServiceReplicaResponse(new ServiceReplicaResponse(this.secureIdentifierList.size(), secureIdentifierSubList));
    }
}