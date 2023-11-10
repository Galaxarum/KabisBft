package kabis.art_exhibition.kafka_only;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.LocalTime;
import java.util.Arrays;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class ArtEstate extends ArtExhibitionConsumer {
    protected ArtEstate(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    public static void main(String[] args) throws InterruptedException {
        // -- CHECK IF ALL ARGUMENTS ARE PRESENT --
        if (args.length != 5) {
            System.out.print("--ERROR-- \nUSAGE: ArtEstate <clientId> <numberOfArtExhibitions> <totalNumberOfAlarms> <falseAlarmsPercentage> <alarmsNotTriggeredPercentage>");
            System.exit(1);
        }
        // -- PRIMING KAFKA, CREATING TOPICS --
        Thread.sleep(15000);
        // -- RUN ART-ESTATE INSTANCE --
        new ArtEstate(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3]), parseInt(args[4])).run();
    }

    private void run() {
        Properties properties = readProperties();
        properties.setProperty("client.id", String.valueOf(getClientId()));

        KafkaConsumer<Integer, String> artEstateConsumer = new KafkaConsumer<>(properties);
        artEstateConsumer.subscribe(TOPICS);
        System.out.println("[ArtEstate - Kafka Only] Kabis Consumer created");

        System.out.println("[ArtEstate - Kafka Only] Reading alarms");
        int numberOfAssignedPartitions = artEstateConsumer.assignment().size();
        System.out.println("[ArtEstate - Kafka Only] Number of assigned exhibitions: " + numberOfAssignedPartitions);
        int recordsToRead = ((getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * 2 + getNumberOfUncaughtBreaches()) * numberOfAssignedPartitions;
        LocalTime[] timeResults = pollAndMeasure(artEstateConsumer, recordsToRead);

        artEstateConsumer.close();
        System.out.println("[ArtEstate - Kafka Only] DONE! Consumer Closed - Saving experiments");

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TOTAL ALARMS", "START TIME", "END TIME"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(recordsToRead), timeResults[0].toString(), timeResults[1].toString()));
        System.out.println("[ArtEstate - Kafka Only] Experiments persisted!");
    }
}
