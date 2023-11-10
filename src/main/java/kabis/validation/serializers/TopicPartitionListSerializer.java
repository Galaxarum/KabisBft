package kabis.validation.serializers;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TopicPartitionListSerializer {
    /**
     * Serializes a list of TopicPartitions to a byte array.
     *
     * @param topicPartitions the list of TopicPartitions to serialize.
     * @return the serialized list of TopicPartitions.
     */
    public static byte[] serializeTopicPartitionList(List<TopicPartition> topicPartitions) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(topicPartitions.size()).array());

            for (TopicPartition topicPartition : topicPartitions) {
                byte[] topicBytes = topicPartition.topic().getBytes();
                bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(topicBytes.length).array());
                bytes.writeBytes(topicBytes);
                bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(topicPartition.partition()).array());
            }

            return bytes.toByteArray();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    /**
     * Deserializes a list of TopicPartitions from a byte array.
     *
     * @param serializedBytes the byte array to deserialize.
     * @return the deserialized list of TopicPartitions.
     */
    public static List<TopicPartition> deserializeTopicPartitionList(byte[] serializedBytes) {
        List<TopicPartition> topicPartitions;
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(serializedBytes)) {
            int topicPartitionsSize = ByteBuffer.wrap(bytes.readNBytes(Integer.BYTES)).getInt();
            topicPartitions = new ArrayList<>(topicPartitionsSize);

            for (int i = 0; i < topicPartitionsSize; i++) {
                int topicLength = ByteBuffer.wrap(bytes.readNBytes(Integer.BYTES)).getInt();
                String topic = new String(bytes.readNBytes(topicLength));
                int partition = ByteBuffer.wrap(bytes.readNBytes(Integer.BYTES)).getInt();
                topicPartitions.add(new TopicPartition(topic, partition));
            }

            return topicPartitions;
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
