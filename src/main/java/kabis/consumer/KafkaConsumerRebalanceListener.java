package kabis.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

public class KafkaConsumerRebalanceListener<K, V> implements ConsumerRebalanceListener {

    private final KafkaPollingThread<K, V> kafkaPollingThread;
    private final int replicaIndex;
    private final Logger log;

    public KafkaConsumerRebalanceListener(KafkaPollingThread<K, V> kafkaPollingThread, int replicaIndex) {
        this.kafkaPollingThread = kafkaPollingThread;
        this.replicaIndex = replicaIndex;
        this.log = LoggerFactory.getLogger(KafkaConsumerRebalanceListener.class);
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        this.log.info("Replica {}, partitions revoked: {}", this.replicaIndex, partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        this.log.info("Replica {}, partitions assigned: {}", this.replicaIndex, partitions);
        this.kafkaPollingThread.updateAssignedPartitions(this.replicaIndex, new ArrayList<>(partitions));
    }
}