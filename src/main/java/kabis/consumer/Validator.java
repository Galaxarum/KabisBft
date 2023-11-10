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
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> verify(List<SecureIdentifier> sids, Duration duration) {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> mapTopicPartitionValidatedRecords = new HashMap<>();
        for (SecureIdentifier sid : sids) {
            List<ConsumerRecord<K, V>> topicPartitionValidatedRecords = mapTopicPartitionValidatedRecords.computeIfAbsent(sid.getTopicPartition(), tp -> new LinkedList<>());
            List<ConsumerRecord<K, MessageWrapper<V>>> recordsFromDifferentReplicas = this.kafkaPollingThread.poll(sid.getTopicPartition(), sid.getSenderId(), duration);
            for (ConsumerRecord<K, MessageWrapper<V>> record : recordsFromDifferentReplicas) {
                if (sid.checkProof(record)) {
                    MessageWrapper<V> wrapper = record.value();
                    ConsumerRecord<K, V> deserializedRecord = new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), record.timestamp(),
                            record.timestampType(), record.serializedKeySize(), record.serializedValueSize(), record.key(), wrapper.getValue(),
                            record.headers(), record.leaderEpoch());
                    topicPartitionValidatedRecords.add(deserializedRecord);
                    break;
                }
            }
        }
        return mapTopicPartitionValidatedRecords;
    }
}
