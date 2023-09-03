package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

public abstract class ArtExhibitionConsumer extends ArtExhibitionClient {
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1); //ZERO if latency test, 1s otherwise
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

    /**
     * Polls and measures the time it takes to receive all the messages.
     *
     * @param consumer      the consumer to poll from
     * @param recordsToRead the number of records to read
     * @return the start and end time of the polling
     */
    protected LocalTime[] pollAndMeasure(KabisConsumer<Integer, String> consumer, Integer recordsToRead) {
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

    /**
     * Polls and measures the latency of the messages received.
     *
     * @param consumer      the consumer to poll from
     * @param recordsToRead the number of records to read
     * @return the average latency of the messages received
     */
    protected long pollAndMeasureLatencyTest(KabisConsumer<Integer, String> consumer, Integer recordsToRead) {
        int i = 0;
        List<Long> latencies = new ArrayList<>();
        System.out.println("[pollAndMeasureLatencyTest]: recordsToRead: " + recordsToRead + " with POLL_TIMEOUT: " + POLL_TIMEOUT);
        while (i < recordsToRead) {
            System.out.println("[pollAndMeasureLatencyTest]: Pulling....");
            ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<Integer, String> record : records) {
                i += 1;
                LocalTime timestamp = LocalTime.parse(record.value());
                long latency = Duration.between(timestamp, LocalTime.now()).toNanos();
                System.out.println("[pollAndMeasureLatencyTest]: Latency: " + latency + "ns" + " exhibition: " + record.partition() + " VALIDATED RECORDS until now: " + i);
                latencies.add(latency);
            }
        }
        System.out.println("[pollAndMeasureLatencyTest]: All messages read!");
        return latencies.stream().mapToLong(Long::longValue).sum() / latencies.size();
    }
}
