package kabis.validation;

import kabis.crypto.KeyStoreHelper;
import kabis.storage.MessageWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public abstract class SecureIdentifier {
    /**
     * If true, the SecureIdentifier will use SignatureSid, otherwise it will use EqualsSid.
     */
    private static boolean USE_SIGNATURES = true;
    /**
     * The proof of the record, as a byte array.
     */
    protected final byte[] proof;
    /**
     * The topic of the record.
     */
    protected final String topic;
    /**
     * The partition of the record.
     */
    protected final int partition;
    /**
     * The senderId of the record.
     */
    protected final int senderId;

    /**
     * Creates a new SecureIdentifier.
     *
     * @param proof     the proof of the record, as a byte array
     * @param topic     the topic of the record
     * @param partition the partition of the record
     * @param senderId  the senderId of the record
     */
    protected SecureIdentifier(byte[] proof, String topic, int partition, int senderId) {
        this.proof = proof;
        this.topic = topic;
        this.partition = partition;
        this.senderId = senderId;
    }

    public static void setUseSignatures(boolean useSignatures) {
        USE_SIGNATURES = useSignatures;
    }

    /**
     * Creates a new SecureIdentifier.
     * If USE_SIGNATURES is true, it creates a SignatureSid, otherwise it creates an EqualsSid.
     *
     * @param key       the key of the record
     * @param value     the value of the record
     * @param topic     the topic of the record
     * @param partition the partition of the record
     * @param senderId  the senderId of the record
     * @return a new SecureIdentifier
     */
    public static SecureIdentifier factory(int key, String value, String topic, int partition, int senderId) {
        if (USE_SIGNATURES) {
            byte[] toSign = SignatureSid.serializeBeforeSigning(key, value, topic, partition);
            byte[] signature = KeyStoreHelper.getInstance().signBytes(senderId, toSign);
            return new SignatureSid(signature, topic, partition, senderId);
        } else return new EqualsSid(value.getBytes(), topic, partition, senderId);
    }

    /**
     * Deserializes a SecureIdentifier from a byte array.
     *
     * @param serialized the byte array to deserialize
     * @return the deserialized SecureIdentifier as a SecureIdentifier object
     */
    public static SecureIdentifier deserialize(byte[] serialized) {
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized)) {
            int proofLen = ByteBuffer.wrap(bytes.readNBytes(Integer.BYTES)).getInt();
            byte[] proof = bytes.readNBytes(proofLen);

            int topicLen = ByteBuffer.wrap(bytes.readNBytes(Integer.BYTES)).getInt();
            String topic = new String(bytes.readNBytes(topicLen));

            int partition = bytes.read();

            int senderId = ByteBuffer.wrap(bytes.readAllBytes()).getInt();

            if (USE_SIGNATURES) return new SignatureSid(proof, topic, partition, senderId);
            else return new EqualsSid(proof, topic, partition, senderId);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    public TopicPartition getTopicPartition() {
        return new TopicPartition(this.topic, this.partition);
    }

    public int getSenderId() {
        return this.senderId;
    }

    public abstract <K extends Integer, V extends String> boolean checkProof(ConsumerRecord<K, MessageWrapper<V>> wrapper);

    /**
     * Serializes the SecureIdentifier.
     *
     * @return the serialized SecureIdentifier as a byte array
     */
    public byte[] serialize() {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(this.proof.length).array());
            bytes.writeBytes(this.proof);

            byte[] topicBytes = this.topic.getBytes();
            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(topicBytes.length).array());
            bytes.writeBytes(topicBytes);

            bytes.write(this.partition);

            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(this.senderId).array());

            return bytes.toByteArray();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public String toString() {
        return "SecureIdentifier{" +
                "proof=" + Arrays.toString(this.proof) +
                ", topic='" + this.topic + '\'' +
                ", partition=" + this.partition +
                ", senderId=" + this.senderId +
                '}';
    }
}

/**
 * A SecureIdentifier that does not use signatures.
 */
class EqualsSid extends SecureIdentifier {

    /**
     * Creates a new EqualsSid, a SecureIdentifier that does not use signatures.
     * It sets the value of the record as the proof.
     *
     * @param value     the value of the record
     * @param topic     the topic of the record
     * @param partition the partition of the record
     * @param senderId  the senderId of the record
     */
    EqualsSid(byte[] value, String topic, int partition, int senderId) {
        super(value, topic, partition, senderId);
    }

    /**
     * Checks if the value of the record is equal to the proof of the SecureIdentifier.
     *
     * @param record the record to check
     * @return true if the value of the record is equal to the proof of the SecureIdentifier, false otherwise
     */
    @Override
    public <K extends Integer, V extends String> boolean checkProof(ConsumerRecord<K, MessageWrapper<V>> record) {
        return new String(this.proof).contentEquals(record.value().getValue());
    }
}

/**
 * A SecureIdentifier that uses signatures.
 */
class SignatureSid extends SecureIdentifier {
    /**
     * Creates a new SignatureSid, a SecureIdentifier that uses signatures.
     * It sets the signature of the record as the proof.
     *
     * @param signature the signature of the record
     * @param topic     the topic of the record
     * @param partition the partition of the record
     * @param senderId  the senderId of the record
     */
    SignatureSid(byte[] signature, String topic, int partition, int senderId) {
        super(signature, topic, partition, senderId);
    }

    /**
     * Checks if the signature of the record is valid.
     *
     * @param record the record to check
     * @return true if the signature of the record is valid, false otherwise
     */
    @Override
    public <K extends Integer, V extends String> boolean checkProof(ConsumerRecord<K, MessageWrapper<V>> record) {
        MessageWrapper<V> wrapper = record.value();
        String topic = record.topic();
        int partition = record.partition();
        K key = record.key();
        V value = wrapper.getValue();
        byte[] toVerify = serializeBeforeSigning(key, value, topic, partition);
        return KeyStoreHelper.getInstance().validateSignature(this.senderId, toVerify, this.proof);
    }

    /**
     * Serializes the key, value, topic and partition of the record.
     *
     * @param key       the key of the record
     * @param value     the value of the record
     * @param topic     the topic of the record
     * @param partition the partition of the record
     * @return the serialized key, value, topic and partition of the record as a byte array
     */
    public static byte[] serializeBeforeSigning(int key, String value, String topic, int partition) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            bytes.write(key);
            bytes.writeBytes(value.getBytes());
            bytes.writeBytes(topic.getBytes());
            bytes.write(partition);
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}

