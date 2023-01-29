package kabis.benchmark;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BenchmarkResult {
    public static final List<String> TOPICS = IntStream.range(0,12)
            .mapToObj(i-> String.format("topic-%d", i))
            .collect(Collectors.toList());
    public static final String HEADER_STRING = "numOperations,payloadSize[B],validatedTopic,totalTopics,totalTime[ns]\n";

    public static String buildThroughputString(int numOps,int payloadSize,int validated,long passedTime){
        return String.format("%d,%d,%d,%d,%d\n",numOps,payloadSize,validated,TOPICS.size(),passedTime);
    }

    public static void storeThroughputToDisk(String throughputString) throws IOException {
        var resultDir = new File("result");
        resultDir.mkdir();
        var throughputFile = new File(resultDir,"throughput.csv");
        var existed = throughputFile.exists();
        if(!existed) throughputFile.createNewFile();
        try (var out = new FileWriter(throughputFile,true)){
            if(!existed) out.write(HEADER_STRING);
            out.write(throughputString);
            out.flush();
        }
    }

}
