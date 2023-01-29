package kabis.producer;

import kabis.UpdateTopologyI;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.DuplicateResourceException;

import java.time.Duration;

public interface KabisProducerI<K,V> extends UpdateTopologyI {
    void push(ProducerRecord<K,V> record);
    void flush();
    void close();
    void close(Duration duration);
}
