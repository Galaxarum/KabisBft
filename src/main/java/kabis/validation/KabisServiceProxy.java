package kabis.validation;

import bftsmart.tom.ServiceProxy;
import kabis.validation.serializers.ServiceReplicaResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static kabis.validation.serializers.ServiceReplicaResponse.deserializeServiceReplicaResponse;
import static kabis.validation.serializers.TopicPartitionListSerializer.serializeTopicPartitionList;

/**
 * A singleton proxy for the KabisServiceReplica.
 * The KabisServiceProxy is used to push and pull SecureIdentifiers to and from the KabisServiceReplica.
 * It's a singleton, since an application can have multiple KabisProducers and KabisConsumers, but only one KabisServiceProxy.
 */
public class KabisServiceProxy {
    private static final KabisServiceProxy instance = new KabisServiceProxy();
    private boolean isInitialized = false;
    private ServiceProxy bftServiceProxy;
    private boolean orderedPulls;
    private int nextPullIndex = 0;

    public static KabisServiceProxy getInstance() {
        return instance;
    }

    /**
     * Initializes the KabisServiceProxy with the given id and orderedPulls flag,
     * only if it has not been initialized before.
     *
     * @param id           The id of the KabisServiceReplica to connect to
     * @param orderedPulls Whether to pull SecureIdentifiers in order or not
     */
    public synchronized void init(int id, boolean orderedPulls) {
        if (!this.isInitialized) {
            this.bftServiceProxy = new ServiceProxy(id);
            this.orderedPulls = orderedPulls;
            this.isInitialized = true;
        }
    }

    /**
     * Resets the nextPullIndex to 0.
     */
    public void resetNextPullIndex() {
        this.nextPullIndex = 0;
    }

    /**
     * Pushes a SecureIdentifier to the KabisServiceReplica
     *
     * @param sid The SecureIdentifier to push
     */
    public void push(SecureIdentifier sid) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            bytes.write(OPS.PUSH.ordinal());
            bytes.writeBytes(sid.serialize());
            if (this.orderedPulls)
                this.bftServiceProxy.invokeOrdered(bytes.toByteArray());
            else
                this.bftServiceProxy.invokeUnordered(bytes.toByteArray());
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Pulls SecureIdentifiers from the KabisServiceReplica that have not been pulled before and returns them as a list.
     * If no SecureIdentifiers are available, an empty list is returned.
     * <p>
     * If orderedPulls is set to true, the pull is ordered. Otherwise, it is unordered.
     * Ordered pulls are slower, but guarantee that the SecureIdentifiers are returned in the order they were pushed.
     * Unordered pulls are faster, but do not guarantee the order of the SecureIdentifiers.
     *
     * @param topicPartitions The topic partitions to pull from
     * @return A list of SecureIdentifiers
     */
    public List<SecureIdentifier> pull(List<TopicPartition> topicPartitions) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            bytes.write(OPS.PULL.ordinal());
            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(this.nextPullIndex).array());
            bytes.writeBytes(serializeTopicPartitionList(topicPartitions));
            byte[] request = bytes.toByteArray();
            byte[] responseBytes = this.orderedPulls ?
                    this.bftServiceProxy.invokeOrdered(request) :
                    this.bftServiceProxy.invokeUnordered(request);
            ServiceReplicaResponse result = deserializeServiceReplicaResponse(responseBytes);
            this.nextPullIndex = result.getLastIndex();
            return result.getSidList();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
