package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;

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

        KabisConsumer<Integer, String> artEstateConsumer = new KabisConsumer<>(properties);
        artEstateConsumer.subscribe(TOPICS);
        artEstateConsumer.updateTopology(TOPICS);
        System.out.println("[ArtEstate] Kabis Consumer created");

        System.out.println("[ArtEstate] Reading alarms");
        int numberOfAssignedPartitions = artEstateConsumer.getAssignedPartitions().size();
        System.out.println("[ArtEstate] Number of assigned exhibitions: " + numberOfAssignedPartitions);
        int recordsToRead = ((getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * 2 + getNumberOfUncaughtBreaches()) * numberOfAssignedPartitions;
        long time = pollAndMeasure(artEstateConsumer, recordsToRead);
        artEstateConsumer.close();
        System.out.println("[ArtEstate] DONE! Consumer Closed - Saving experiments");

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TOTAL ALARMS", "TOTAL TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(recordsToRead), Long.toString(time)));
        System.out.println("[ArtEstate] Experiments persisted!");
    }
}
