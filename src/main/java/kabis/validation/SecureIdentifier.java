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
    protected final byte[] proof;
    protected final String topic;
    protected final int partition;
    protected final int senderId;
    private static boolean USE_SIGNATURES = true;

    public static void setUseSignatures(boolean useSignatures){
        USE_SIGNATURES = useSignatures;
    }

    protected SecureIdentifier(byte[] proof, String topic, int partition,int senderId) {
        this.proof = proof;
        this.topic = topic;
        this.partition = partition;
        this.senderId = senderId;
    }

    public static SecureIdentifier factory(int key, String value, String topic, int partition,int senderId){
        if(USE_SIGNATURES){
            byte[] toSign = SignatureSid.serializeBeforeSigning(key, value, topic, partition);
            byte[] signature = KeyStoreHelper.getInstance().signBytes(senderId,toSign);
            return new SignatureSid(signature,topic,partition,senderId);
        }
        else return new EqualsSid(value.getBytes(),topic,partition,senderId);
    }

    public static SecureIdentifier deserialize(byte[] serialized){
        try(var bytes = new ByteArrayInputStream(serialized)){
            var proofLen = ByteBuffer.wrap(bytes.readNBytes(Integer.BYTES)).getInt();
            var proof = bytes.readNBytes(proofLen);

            var topicLen = ByteBuffer.wrap(bytes.readNBytes(Integer.BYTES)).getInt();
            var topic = new String(bytes.readNBytes(topicLen));

            var partition = bytes.read();

            var senderId = ByteBuffer.wrap(bytes.readAllBytes()).getInt();

            if(USE_SIGNATURES) return new SignatureSid(proof, topic, partition, senderId);
            else return new EqualsSid(proof,topic,partition,senderId);
        }catch (IOException e){
            throw new SerializationException(e);
        }
    }

    public abstract <K extends Integer, V extends String> boolean checkProof(ConsumerRecord<K,MessageWrapper<V>> wrapper);

    public byte[] serialize(){
        try(var bytes = new ByteArrayOutputStream()){
            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(proof.length).array());
            bytes.writeBytes(proof);

            var topicBytes = topic.getBytes();
            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(topicBytes.length).array());
            bytes.writeBytes(topicBytes);

            bytes.write(partition);

            bytes.writeBytes(ByteBuffer.allocate(Integer.BYTES).putInt(senderId).array());

            return bytes.toByteArray();
        }catch (IOException e){
            throw new SerializationException(e);
        }
    }

    public TopicPartition topicPartition(){
        return new TopicPartition(topic,partition);
    }

    public int senderId(){
        return senderId;
    }

    @Override
    public String toString() {
        return "SecureIdentifier{" +
                "proof=" + Arrays.toString(proof) +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", senderId=" + senderId +
                '}';
    }
}

class EqualsSid extends SecureIdentifier{

    EqualsSid(byte[] value, String topic, int partition,int senderId) {
        super(value, topic, partition,senderId);
    }

    @Override
    public <K extends Integer, V extends String> boolean checkProof(ConsumerRecord<K,MessageWrapper<V>> record) {
        return new String(proof).contentEquals(record.value().getValue());
    }
}

class SignatureSid extends SecureIdentifier{
    SignatureSid(byte[] signature, String topic, int partition,int senderId) {
        super(signature, topic, partition,senderId);
    }

    public static byte[] serializeBeforeSigning(int key,String value, String topic, int partition){
        try (var bytes = new ByteArrayOutputStream()){
            bytes.write(key);
            bytes.writeBytes(value.getBytes());
            bytes.writeBytes(topic.getBytes());
            bytes.write(partition);
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public <K extends Integer, V extends String> boolean checkProof(ConsumerRecord<K,MessageWrapper<V>> record) {
        var wrapper = record.value();
        var topic = record.topic();
        var partition = record.partition();
        var key = record.key();
        var value = wrapper.getValue();
        var toVerify = serializeBeforeSigning(key,value,topic,partition);
        return KeyStoreHelper.getInstance().validateSignature(senderId,toVerify,proof);
    }
}

