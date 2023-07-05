package kabis.art_exhibition;

import kabis.consumer.KabisConsumer;

import java.util.Arrays;

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
        Thread.sleep(30000);
        // -- RUN ENSURE INSTANCE --
        new Ensure(parseInt(args[0]), parseInt(args[1]), parseInt(args[2]), parseInt(args[3]), parseInt(args[4])).run();
        // -- KILL THE BENCHMARK AFTER run() --
        Thread.sleep(60000);
        System.exit(0);
    }

    private void run() {
        KabisConsumer<Integer, String> ensureConsumer = new KabisConsumer<>(getProperties());
        ensureConsumer.subscribe(TOPICS);
        ensureConsumer.updateTopology(TOPICS);
        System.out.println("[Ensure] Kabis Consumer created");

        System.out.println("[Ensure] Reading alarms");
        // * getNumberOfArtExhibitions() will be removed when scaling on multiple consumers within the same consumer group,
        // every consumer will only read its own exhibition
        int recordsToRead = ((getNumberOfTrueAlarms() + getNumberOfFalseAlarms()) * 2 + getNumberOfUncaughtBreaches()) * getNumberOfArtExhibitions();
        long time = pollAndMeasure(ensureConsumer, recordsToRead);
        ensureConsumer.close();
        System.out.println("[Ensure] DONE! Consumer Closed - Saving experiments");

        ArtExhibitionBenchmarkResult.storeThroughputToDisk(Arrays.asList("#EXHIBITIONS", "#TOTAL ALARMS", "TOTAL TIME [ns]"),
                Arrays.asList(Integer.toString(getNumberOfArtExhibitions()), Integer.toString(recordsToRead), Long.toString(time)));
        System.out.println("[Ensure] Experiments persisted!");
    }
}
