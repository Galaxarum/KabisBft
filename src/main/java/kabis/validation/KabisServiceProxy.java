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

    public void push(SecureIdentifier sid) {
        try (var bytes = new ByteArrayOutputStream()) {
            bytes.write(OPS.PUSH.ordinal());
            bytes.writeBytes(sid.serialize());
            bftServiceProxy.invokeOrdered(bytes.toByteArray());
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    public List<SecureIdentifier> pull() {
        try (var bytes = new ByteArrayOutputStream()) {
            bytes.write(OPS.PULL.ordinal());
            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(nextPullIndex).array());
            var request = bytes.toByteArray();
            var responseBytes = orderedPulls ?
                    bftServiceProxy.invokeOrdered(request) :
                    bftServiceProxy.invokeUnordered(request);
            if (responseBytes == null || responseBytes.length == 0) {
                System.out.println("[KabisServiceProxy] pull method returns responseBytes as null or with length 0");
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {
                }
                return List.of();
            }
            var result = KabisServiceReplica.deserializeSidList(responseBytes);
            nextPullIndex += result.size();
            return result;
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    public KabisServiceProxy(int id, boolean orderedPulls) {
        this.bftServiceProxy = new ServiceProxy(id);
        this.orderedPulls = orderedPulls;
    }

    public KabisServiceProxy(int id) {
        this(id, false);
    }

}
