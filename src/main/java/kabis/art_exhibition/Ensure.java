package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;

import java.util.Arrays;
import java.util.Properties;

import static java.lang.Integer.parseInt;

public class Ensure extends ArtExhibitionConsumer {
    protected Ensure(Integer clientId, Integer numberOfArtExhibitions, Integer numberOfTrueAlarms, Integer numberOfFalseAlarms, Integer numberOfUncaughtBreaches) {
        super(clientId, numberOfArtExhibitions, numberOfTrueAlarms, numberOfFalseAlarms, numberOfUncaughtBreaches);
    }

    public static void main(String[] args) throws InterruptedException {
        // -- CHECK IF ALL ARGUMENTS ARE PRESENT --
        if (args.length != 5) {
            System.out.print("--ERROR-- \nUSAGE: Ensure <clientId> <numberOfArtExhibitions> <totalNumberOfAlarms> <falseAlarmsPercentage> <alarmsNotTriggeredPercentage>");
            System.exit(1);
        }
        // -- PRIMING KAFKA, CREATING TOPICS --
        Thread.sleep(15000);
        // -- RUN ENSURE INSTANCE --
        new Ensure(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3]), parseInt(args[4])).run();
    }

    private void run() {
        Properties properties = readProperties();
        properties.setProperty("client.id", String.valueOf(getClientId()));

        KabisConsumer<Integer, String> ensureConsumer = new KabisConsumer<>(properties);
        ensureConsumer.subscribe(TOPICS);
        ensureConsumer.updateTopology(TOPICS);
        System.out.println("[Ensure] Kabis Consumer created");

        System.out.println("[Ensure] Reading alarms");
        int numberOfAssignedPartitions = ensureConsumer.getAssignedPartitions().size();
        System.out.println("[Ensure] Number of assigned exhibitions: " + numberOfAssignedPartitions);
        int recordsToRead = ((getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * 2 + getNumberOfUncaughtBreaches()) * numberOfAssignedPartitions;
        long time = pollAndMeasure(ensureConsumer, recordsToRead);
        ensureConsumer.close();
        System.out.println("[Ensure] DONE! Consumer Closed - Saving experiments");

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TOTAL ALARMS", "TOTAL TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(recordsToRead), Long.toString(time)));
        System.out.println("[Ensure] Experiments persisted!");
    }
}
