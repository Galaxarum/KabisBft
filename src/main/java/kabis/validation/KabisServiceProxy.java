package kabis.validation;

import bftsmart.tom.ServiceProxy;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class KabisServiceProxy {
    private final ServiceProxy bftServiceProxy;
    private final boolean orderedPulls;
    private int nextPullIndex = 0;

    /**
     * Creates a new KabisServiceProxy
     *
     * @param id The id of the service proxy
     */
    public KabisServiceProxy(int id) {
        this(id, false);
    }

    /**
     * Creates a new KabisServiceProxy
     *
     * @param id           The id of the service proxy
     * @param orderedPulls Whether to use ordered pulls
     */
    public KabisServiceProxy(int id, boolean orderedPulls) {
        this.bftServiceProxy = new ServiceProxy(id);
        this.orderedPulls = orderedPulls;
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
            this.bftServiceProxy.invokeOrdered(bytes.toByteArray());
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
     * @return A list of SecureIdentifiers
     */
    public List<SecureIdentifier> pull() {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            bytes.write(OPS.PULL.ordinal());
            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(this.nextPullIndex).array());
            byte[] request = bytes.toByteArray();
            byte[] responseBytes = this.orderedPulls ?
                    this.bftServiceProxy.invokeOrdered(request) :
                    this.bftServiceProxy.invokeUnordered(request);
            if (responseBytes == null || responseBytes.length == 0) {
                //TODO: Remove this sleep?
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {
                }
                return List.of();
            }
            List<SecureIdentifier> result = KabisServiceReplica.deserializeSidList(responseBytes);
            //TODO: Remove this print
            System.out.println("Pulled " + result.size() + " SIDs, nextPullIndex = " + this.nextPullIndex);
            this.nextPullIndex += result.size();
            return result;
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

}
