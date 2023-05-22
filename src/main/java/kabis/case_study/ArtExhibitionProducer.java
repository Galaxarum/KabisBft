package kabis.case_study;

import kabis.producer.KabisProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static java.lang.Integer.parseInt;

public abstract class ArtExhibitionProducer {
    /**
     * ID of the Art Exhibition.
     */
    private final String topic;
    private final Integer totalNumberOfAlarms;
    /**
     * Percentage of false alarms triggered.
     */
    private final Float falseAlarmsPercentage;
    /**
     * Percentage of real breaches not caught by alarms.
     */
    private final Float alarmsNotTriggeredPercentage;

    public ArtExhibitionProducer(String topic, Integer totalNumberOfAlarms, Float falseAlarmsPercentage, Float alarmsNotTriggeredPercentage) {
        this.topic = topic;
        this.totalNumberOfAlarms = totalNumberOfAlarms;
        this.falseAlarmsPercentage = falseAlarmsPercentage;
        this.alarmsNotTriggeredPercentage = alarmsNotTriggeredPercentage;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getTotalNumberOfAlarms() {
        return totalNumberOfAlarms;
    }

    public Float getFalseAlarmsPercentage() {
        return falseAlarmsPercentage;
    }

    public Float getAlarmsNotTriggeredPercentage() {
        return alarmsNotTriggeredPercentage;
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
