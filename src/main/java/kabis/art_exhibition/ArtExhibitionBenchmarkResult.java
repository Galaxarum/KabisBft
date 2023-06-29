package kabis.art_exhibition;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ArtExhibitionBenchmarkResult {
    protected static void storeThroughputToDisk(List<String> columns, List<String> values) {
        String header = "", results = "";

        for (String column : columns) {
            header = header.concat(column + ",");
        }
        header = header.substring(0, header.length() - 1).concat("\n");
        for (String value : values) {
            results = results.concat(value + ",");
        }
        results = results.substring(0, results.length() - 1).concat("\n");

        File resultDir = new File("result");
        resultDir.mkdir();
        File throughputFile = new File(resultDir, "throughput.csv");
        boolean existed = throughputFile.exists();

        try {
            if (!existed) throughputFile.createNewFile();
            FileWriter out = new FileWriter(throughputFile, true);
            if (!existed) out.write(header);
            out.write(results);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
