package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

public abstract class ArtExhibitionConsumer {
    private final Integer numberOfArtExhibitions;
    private final Integer numberOfTrueAlarms;
    private final Integer numberOfFalseAlarms;
    private final Integer numberOfUncaughtBreaches;

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    public ArtExhibitionConsumer(Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        this.numberOfArtExhibitions = numberOfArtExhibitions;
        this.numberOfTrueAlarms = numberOfTrueAlarms;
        this.numberOfFalseAlarms = numberOfFalseAlarms;
        this.numberOfUncaughtBreaches = numberOfUncaughtBreaches;
    }

    public Integer getNumberOfArtExhibitions() {
        return numberOfArtExhibitions;
    }

    public Integer getNumberOfTrueAlarms() {
        return numberOfTrueAlarms;
    }

    public Integer getNumberOfFalseAlarms() {
        return numberOfFalseAlarms;
    }

    public Integer getNumberOfUncaughtBreaches() {
        return numberOfUncaughtBreaches;
    }

    protected long pollAndMeasure(KabisConsumer<Integer, String> consumer, Integer recordsToRead) {
        int i = 0;
        long t1 = System.nanoTime();
        while (i < recordsToRead) {
            ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<Integer, String> record : records) {
                i += 1;
                System.out.printf("[" + this.getClass().toString() + "] Received alarm from " + record.key() + "\n");
            }
            //i += consumer.poll(POLL_TIMEOUT).count();
        }
        long t2 = System.nanoTime();

        return t2 - t1;
    }
}
