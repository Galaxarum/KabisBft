package kabis.validation.serializers;

import kabis.validation.SecureIdentifier;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class ServiceReplicaResponse {
    private final int lastIndex;
    private final List<SecureIdentifier> sidList;

    public ServiceReplicaResponse(int lastIndex, List<SecureIdentifier> sidList) {
        this.lastIndex = lastIndex;
        this.sidList = sidList;
    }

    /**
     * Serializes a ServiceReplicaResponse.
     *
     * @param serviceReplicaResponse the ServiceReplicaResponse to serialize
     * @return the serialized ServiceReplicaResponse as a byte array
     */
    public static byte[] serializeServiceReplicaResponse(ServiceReplicaResponse serviceReplicaResponse) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            bytes.write(ByteBuffer.allocate(Integer.BYTES).putInt(serviceReplicaResponse.getLastIndex()).array());
            bytes.writeBytes(serializeSidList(serviceReplicaResponse.getSidList()));
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the last index of the SecureIdentifiers.
     *
     * @return the last index of the SecureIdentifiers
     */
    public int getLastIndex() {
        return lastIndex;
    }

    /**
     * Serializes a list of SecureIdentifiers.
     *
     * @param sidList the list of SecureIdentifiers to serialize
     * @return the serialized list of SecureIdentifiers as a byte array
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
     * Returns the list of SecureIdentifiers.
     *
     * @return the list of SecureIdentifiers
     */
    public List<SecureIdentifier> getSidList() {
        return sidList;
    }

    /**
     * Deserializes a ServiceReplicaResponse from a byte array.
     *
     * @param serialized the byte array to deserialize
     * @return the ServiceReplicaResponse
     */
    public static ServiceReplicaResponse deserializeServiceReplicaResponse(byte[] serialized) {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized)) {
            int lastIndex = ByteBuffer.wrap(bytes.readNBytes(Integer.BYTES)).getInt();
            List<SecureIdentifier> res = deserializeSidList(bytes.readAllBytes());
            return new ServiceReplicaResponse(lastIndex, res);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Deserializes a list of SecureIdentifiers from a byte array.
     *
     * @param serialized the byte array to deserialize
     * @return the list of SecureIdentifiers
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
