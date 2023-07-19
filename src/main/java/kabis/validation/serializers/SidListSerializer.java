package kabis.validation.serializers;

import kabis.validation.SecureIdentifier;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class SidListSerializer {
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
