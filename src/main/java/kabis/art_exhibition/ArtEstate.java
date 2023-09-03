package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;

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
        Thread.sleep(20000);
        // -- 14 MIN SLEEP TO WAIT FOR ALL THE MESSAGES TO BE SENT --
        //System.out.println("[ArtEstate] Sleeping for 14 minutes to allow the system to warm up");
        //Thread.sleep(840000);
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

        int numberOfAssignedPartitions = artEstateConsumer.getAssignedPartitions().size();
        System.out.println("[ArtEstate] Number of assigned exhibitions: " + numberOfAssignedPartitions);


        // *** STANDARD BEHAVIOUR ***
        System.out.println("[ArtEstate] Reading alarms");
        int recordsToRead = ((getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * 2 + getNumberOfUncaughtBreaches()) * numberOfAssignedPartitions;
        LocalTime[] timeResults = pollAndMeasure(artEstateConsumer, recordsToRead);

        /*
        // *** LATENCY/THROUGHPUT EXPERIMENT ***
        int recordsToRead = getNumberOfTrueAlarms() * numberOfAssignedPartitions;
        long avgLatency = pollAndMeasureLatencyTest(artEstateConsumer, recordsToRead);
        */

        artEstateConsumer.close();
        System.out.println("[ArtEstate] DONE! Consumer Closed - Saving experiments");


        // *** STANDARD BEHAVIOUR SAVE ***
        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TOTAL ALARMS", "START TIME", "END TIME"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(recordsToRead), timeResults[0].toString(), timeResults[1].toString()));


        /*
        // *** LATENCY/THROUGHPUT EXPERIMENT SAVE ***
        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TOTAL ALARMS", "AVG LATENCY [ns]"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(recordsToRead), Long.toString(avgLatency)));
        */
        System.out.println("[ArtEstate] Experiments persisted!");
    }
}
