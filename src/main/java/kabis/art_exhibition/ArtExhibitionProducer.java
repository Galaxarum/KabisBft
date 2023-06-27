package kabis.art_exhibition;

import kabis.producer.KabisProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public abstract class ArtExhibitionProducer {
    private final Integer clientId;
    private final Integer numberOfArtExhibitions;
    private final Integer numberOfTrueAlarms;
    private final Integer numberOfFalseAlarms;
    private final Integer numberOfUncaughtBreaches;


    public ArtExhibitionProducer(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        this.clientId = clientId;
        this.numberOfArtExhibitions = numberOfArtExhibitions;
        this.numberOfTrueAlarms = numberOfTrueAlarms;
        this.numberOfFalseAlarms = numberOfFalseAlarms;
        this.numberOfUncaughtBreaches = numberOfUncaughtBreaches;
    }

    public ArtExhibitionProducer(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms) {
        this.clientId = clientId;
        this.numberOfArtExhibitions = numberOfArtExhibitions;
        this.numberOfTrueAlarms = numberOfTrueAlarms;
        this.numberOfFalseAlarms = numberOfFalseAlarms;
        this.numberOfUncaughtBreaches = 0;
    }

    public Integer getClientId() {
        return clientId;
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

    protected long sendAndMeasure(KabisProducer<Integer, String> producer, Integer numberOfAlarms, String message) {
        long t1 = System.nanoTime();
        System.out.println("[sendAndMeasure]: numberOfArtExhibitions: " + numberOfArtExhibitions + " numberOfAlarms:" + numberOfAlarms);
        for (int artExhibitionID = 0; artExhibitionID < numberOfArtExhibitions; artExhibitionID++) {
            for (int i = 0; i < numberOfAlarms; i++) {
                System.out.println("Sending to: " + artExhibitionID);
                ProducerRecord<Integer, String> record = new ProducerRecord<>(Topics.ART_EXHIBITION.toString(), artExhibitionID, message + i);
                record.headers().add("sender", this.getClass().toString().getBytes(StandardCharsets.UTF_8));
                System.out.println("Sending " + record.value() + " to: " + record.key() + " with header: " + record.headers().lastHeader("sender"));
                producer.push(record);
            }
        }
        System.out.println("[sendAndMeasure]: All messages sent!");
        producer.flush();
        long t2 = System.nanoTime();

        return t2 - t1;
    }
}
