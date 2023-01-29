package kabis.consumer;

import kabis.validation.KabisServiceProxy;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KabisConsumer<K extends Integer,V extends String> implements KabisConsumerI<K,V>{

    private final Set<String> validatedTopics = new HashSet<>();
    private final KabisServiceProxy serviceProxy;
    private final KafkaPollingThread<K,V> kafkaPollingThread;
    private final Validator<K,V> validator;

    public KabisConsumer(Properties properties) {
        int clientId = Integer.parseInt(properties.getProperty("client.id"));
        this.serviceProxy = new KabisServiceProxy(clientId);
        this.kafkaPollingThread = new KafkaPollingThread<>(properties);
        this.validator = new Validator<>(kafkaPollingThread);
    }

    @Override
    public void subscribe(Collection<String> topics) {
        kafkaPollingThread.subscribe(topics);
    }

    @Override
    public void unsubscribe() {
        kafkaPollingThread.unsubscribe();
    }

    @Override
    public ConsumerRecords<K,V> poll(Duration duration) {
        var sids = serviceProxy.pull();
        //if(!sids.isEmpty()) System.out.printf("Received %d sids%n", sids.size());
        var validatedRecords = validator.verify(sids);
        //if(!validatedRecords.isEmpty()) System.out.printf("Received %d validated records%n", validatedRecords.values().stream().map(List::size).reduce(Integer::sum).orElse(-1));

        var unvalidatedRecords = kafkaPollingThread.pollUnvalidated(validatedTopics,duration);
        //if(!unvalidatedRecords.isEmpty()) System.out.printf("Received %d unvalidated records%n", unvalidatedRecords.values().stream().map(List::size).reduce(Integer::sum).orElse(-1));

        var mergedMap = Stream.concat(validatedRecords.entrySet().stream(),unvalidatedRecords.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (l1,l2)->Stream.concat(l1.stream(),l2.stream()).collect(Collectors.toList())
                        )
                );

        return new ConsumerRecords<>(mergedMap);
    }

    @Override
    public void close() {
        kafkaPollingThread.close();
    }

    @Override
    public void close(Duration duration) {
        kafkaPollingThread.close(duration);
    }

    @Override
    public void updateTopology(Collection<String> validatedTopics) {
        synchronized (this.validatedTopics){
            this.validatedTopics.clear();
            this.validatedTopics.addAll(validatedTopics);
        }
    }
}
