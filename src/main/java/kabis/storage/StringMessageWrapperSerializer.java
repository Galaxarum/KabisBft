package kabis.storage;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class StringMessageWrapperSerializer implements Serializer<MessageWrapper<String>> {
    @Override
    public byte[] serialize(String topic, MessageWrapper<String> data) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            out.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(data.getSenderId()).array());
            out.writeBytes(data.getValue().getBytes());
            return out.toByteArray();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
