package kabis.art_exhibition.kafka_only;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public abstract class ArtExhibitionProducer extends ArtExhibitionClient {
    private final Integer clientId;
    private final Integer numberOfArtExhibitions;
    private final Integer numberOfTrueAlarms;
    private final Integer numberOfFalseAlarms;
    private final Integer numberOfUncaughtBreaches;


    protected ArtExhibitionProducer(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms) {
        this(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms, 0);
    }

    protected ArtExhibitionProducer(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        this.clientId = clientId;
        this.numberOfArtExhibitions = numberOfArtExhibitions;
        this.numberOfTrueAlarms = numberOfTrueAlarms;
        this.numberOfFalseAlarms = numberOfFalseAlarms;
        this.numberOfUncaughtBreaches = numberOfUncaughtBreaches;
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

    protected Integer getClientId() {
        return clientId;
    }

    protected long sendAndMeasure(KafkaProducer<Integer, String> producer, Integer numberOfAlarms, String message) {
        long t1 = System.nanoTime();
        System.out.println("[sendAndMeasure - Kafka Only]: numberOfArtExhibitions: " + this.numberOfArtExhibitions + " numberOfAlarms:" + numberOfAlarms);
        for (int artExhibitionID = 0; artExhibitionID < this.numberOfArtExhibitions; artExhibitionID++) {
            for (int i = 0; i < numberOfAlarms; i++) {
                ProducerRecord<Integer, String> record = new ProducerRecord<>(Topics.ART_EXHIBITION.toString(), artExhibitionID, artExhibitionID, message + i);
                System.out.println("[sendAndMeasure - Kafka Only]: Sending " + record.value() + " exhibition: " + record.key());
                producer.send(record);
            }
        }
        long t2 = System.nanoTime();
        System.out.println("[sendAndMeasure - Kafka Only]: All messages sent!");
        producer.flush();

        return t2 - t1;
    }
}
