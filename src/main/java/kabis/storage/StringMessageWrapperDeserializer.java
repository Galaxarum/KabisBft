package kabis.storage;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StringMessageWrapperDeserializer implements Deserializer<MessageWrapper<String>> {
    @Override
    public MessageWrapper<String> deserialize(String topic, byte[] data) {
        try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
            int senderId = ByteBuffer.wrap(in.readNBytes(Integer.BYTES)).getInt();
            String value = new String(in.readAllBytes());
            return new MessageWrapper<>(value, senderId);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
