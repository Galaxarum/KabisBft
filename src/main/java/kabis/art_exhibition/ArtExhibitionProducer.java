package kabis.art_exhibition;

import kabis.producer.KabisProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

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

        Properties properties = getProperties();
        properties.setProperty("client.id", String.valueOf(this.clientId));
        setProperties(properties);
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

    protected long sendAndMeasure(KabisProducer<Integer, String> producer, Integer numberOfAlarms, String message) {
        long t1 = System.nanoTime();
        System.out.println("[sendAndMeasure]: numberOfArtExhibitions: " + numberOfArtExhibitions + " numberOfAlarms:" + numberOfAlarms);
        for (int artExhibitionID = 0; artExhibitionID < numberOfArtExhibitions; artExhibitionID++) {
            for (int i = 0; i < numberOfAlarms; i++) {
                ProducerRecord<Integer, String> record = new ProducerRecord<>(Topics.ART_EXHIBITION.toString(), artExhibitionID, message + i);
                record.headers().add("clientId", String.valueOf(getClientId()).getBytes());
                System.out.println("[sendAndMeasure]: Sending " + record.value() + " exhibition: " + record.partition());
                producer.push(record);
            }
        }
        long t2 = System.nanoTime();
        System.out.println("[sendAndMeasure]: All messages sent!");
        producer.flush();

        return t2 - t1;
    }

    protected Integer getClientId() {
        return clientId;
    }
}
