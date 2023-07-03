package kabis.consumer;

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
    //TODO: Check if this is a good value, maybe it should be increased
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
        var map = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
        for (var sid : sids) {
            var list = map.computeIfAbsent(sid.topicPartition(), tp -> new LinkedList<>());
            var elems = kafkaPollingThread.poll(sid.topicPartition(), sid.senderId(), KAFKA_POLL_TIMEOUT);
            for (var record : elems) {
                if (sid.checkProof(record)) {
                    var wrapper = record.value();
                    //TODO: Maybe we should add the headers?
                    list.add(new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), record.key(), wrapper.getValue()));
                    break;
                }
            }
        }
        return map;
    }
}
