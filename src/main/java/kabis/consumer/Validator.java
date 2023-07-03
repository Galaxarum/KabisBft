package kabis.consumer;

import kabis.storage.MessageWrapper;
import kabis.validation.SecureIdentifier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Validator<K extends Integer, V extends String> {
    private final KafkaPollingThread<K, V> kafkaPollingThread;
    private final Duration KAFKA_POLL_TIMEOUT = Duration.ofSeconds(1);

    /**
     * Creates a new Validator.
     *
     * @param kafkaPollingThread the KafkaPollingThread to be used by the Validator
     */
    public Validator(KafkaPollingThread<K, V> kafkaPollingThread) {
        this.kafkaPollingThread = kafkaPollingThread;
    }

    /**
     * Verifies a list of SecureIdentifiers.
     *
     * @param sids the SecureIdentifiers to be verified
     * @return a map of TopicPartitions to lists of ConsumerRecords
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> verify(List<SecureIdentifier> sids) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> map = new HashMap<>();
        for (var sid : sids) {
            List<ConsumerRecord<K, V>> list = map.computeIfAbsent(sid.topicPartition(), tp -> new LinkedList<>());
            List<ConsumerRecord<K, MessageWrapper<V>>> elems = kafkaPollingThread.poll(sid.topicPartition(), sid.senderId(), KAFKA_POLL_TIMEOUT);
            for (ConsumerRecord<K, MessageWrapper<V>> record : elems) {
                if (sid.checkProof(record)) {
                    MessageWrapper<V> wrapper = record.value();
                    // TODO: Add timestamp to the constructor, has of now it is reset to the current time
                    ConsumerRecord<K, V> deserializedRecord = new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), record.key(), wrapper.getValue());
                    record.headers().forEach(h -> deserializedRecord.headers().add(h));
                    list.add(deserializedRecord);
                    break;
                }
            }
        }
        return map;
    }
}
