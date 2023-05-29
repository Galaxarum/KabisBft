package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;

import java.time.Duration;

public abstract class ArtExhibitionConsumer {
    /**
     * ID of the Art Exhibition.
     */
    private final String topic;
    private final Integer numberOfTrueAlarms;
    private final Integer numberOfFalseAlarms;
    private final Integer numberOfUncaughtBreaches;

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    public ArtExhibitionConsumer(String topic, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        this.topic = topic;
        this.numberOfTrueAlarms = numberOfTrueAlarms;
        this.numberOfFalseAlarms = numberOfFalseAlarms;
        this.numberOfUncaughtBreaches = numberOfUncaughtBreaches;
    }

    public String getTopic() {
        return topic;
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
            i += consumer.poll(POLL_TIMEOUT).count();
        }
        long t2 = System.nanoTime();

        return t2 - t1;
    }
}
