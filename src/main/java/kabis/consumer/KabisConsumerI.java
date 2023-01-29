package kabis.consumer;

import kabis.UpdateTopologyI;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collection;

public interface KabisConsumerI<K,V> extends UpdateTopologyI {

    void subscribe(Collection<String> topics);

    void unsubscribe();

    ConsumerRecords<K,V> poll(Duration duration);

    void close();

    void close(Duration duration);
}
