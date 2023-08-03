package kabis.art_exhibition.kafka_only;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.LocalTime;

public abstract class ArtExhibitionConsumer extends ArtExhibitionClient {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
    private final Integer clientId;
    private final Integer numberOfArtExhibitions;
    private final Integer numberOfTrueAlarms;
    private final Integer numberOfFalseAlarms;
    private final Integer numberOfUncaughtBreaches;

    protected ArtExhibitionConsumer(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        this.clientId = clientId;
        this.numberOfArtExhibitions = numberOfArtExhibitions;
        this.numberOfTrueAlarms = numberOfTrueAlarms;
        this.numberOfFalseAlarms = numberOfFalseAlarms;
        this.numberOfUncaughtBreaches = numberOfUncaughtBreaches;
    }

    protected Integer getClientId() {
        return clientId;
    }

    protected Integer getNumberOfArtExhibitions() {
        return numberOfArtExhibitions;
    }

    protected Integer getNumberOfTrueAlarms() {
        return numberOfTrueAlarms;
    }

    protected Integer getNumberOfFalseAlarms() {
        return numberOfFalseAlarms;
    }

    protected Integer getNumberOfUncaughtBreaches() {
        return numberOfUncaughtBreaches;
    }

    protected LocalTime[] pollAndMeasure(KafkaConsumer<Integer, String> consumer, Integer recordsToRead) {
        int i = 0;
        LocalTime startTime = LocalTime.now();
        System.out.println("[pollAndMeasure]: recordsToRead: " + recordsToRead + " with POLL_TIMEOUT: " + POLL_TIMEOUT);
        while (i < recordsToRead) {
            ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<Integer, String> record : records) {
                i += 1;
                System.out.println("[pollAndMeasure]: Received " + record.value() + " exhibition: " + record.partition());
                System.out.println("[pollAndMeasure]: Total VALIDATED RECORDS until now: " + i);
            }
        }
        LocalTime endTime = LocalTime.now();
        System.out.println("[pollAndMeasure]: All messages read!");
        return new LocalTime[]{startTime, endTime};
    }
}