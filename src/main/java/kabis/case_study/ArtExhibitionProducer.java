package kabis.case_study;

import kabis.producer.KabisProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static java.lang.Integer.parseInt;

public abstract class ArtExhibitionProducer {
    /**
     * ID of the Art Exhibition.
     */
    private final String topic;
    private final Integer numberOfTrueAlarms;
    private final Integer numberOfFalseAlarms;
    private final Integer numberOfUncaughtBreaches;

    public ArtExhibitionProducer(String topic, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
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


    protected long sendAndMeasure(KabisProducer<Integer,String> producer, Integer numberOfAlarms, String message){
        long t1 = System.nanoTime();

        for(int i = 0; i < numberOfAlarms; i++) {
            var record = new ProducerRecord<>(this.topic, parseInt(this.topic), message + i);
            producer.push(record);
        }
        producer.flush();
        long t2 = System.nanoTime();

        return t2 - t1;
    }
}
