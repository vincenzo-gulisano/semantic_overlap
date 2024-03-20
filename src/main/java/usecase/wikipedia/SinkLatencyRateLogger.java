package usecase.wikipedia;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.flink.configuration.Configuration;

import util.AvgStat;
import util.CountStat;
import util.TimestampedTuple;

public class SinkLatencyRateLogger {

    public static <T extends TimestampedTuple> void createTimestampStringTupleSinkPerformance(DataStream<T> s,
            String outputFileLatency, String outputFileRate, String outputFile, String statsFolder) {
        createTimestampStringTupleSinkPerformance(s, outputFileLatency, outputFileRate, 50000, 50, outputFile, statsFolder);
    }

    public static <T extends TimestampedTuple> void createTimestampStringTupleSinkPerformance(DataStream<T> s,
            String outputFileLatency, String outputFileRate, long maxLatencyValue, int latencyViolationMaxOccurences,
            String outputFile, String statsFolder) {

        // This sink will throw an exception if a latency higher than a threshold is
        // observed consecutively more then a given number of times

        s.addSink(new RichSinkFunction<T>() {

            private AvgStat logLatency;
            private CountStat logRate;
            private int observedViolations = 0;
            private PrintWriter writer;

            @Override
            public void open(Configuration parameters) throws Exception {
                logLatency = new AvgStat(outputFileLatency, true);
                logRate = new CountStat(outputFileRate, true);
                if (!outputFile.equals("")) {
                    writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));
                }
            }

            @Override
            public void close() throws Exception {
                logLatency.close();
                logRate.close();
                if (writer != null) {
                    writer.close();
                }
            }

            @Override
            public void invoke(T value, Context context) throws Exception {
                if (logLatency.add(System.currentTimeMillis() - value.getStimulus()) >= maxLatencyValue) {
                    observedViolations++;
                    if (observedViolations >= latencyViolationMaxOccurences) {
                        System.out.println("Max latency violated");
                        System.out.println("The sink has observed the max latency " + maxLatencyValue
                                + " being violated at least " + observedViolations + " times! Terminating!");
                        File file = new File(statsFolder, "latencyexception.txt");

                        try {
                            file.createNewFile();
                        } catch (IOException e) {
                            System.err.println("An error occurred: " + e.getMessage());
                        }
                        System.exit(13);

                    }
                }
                logRate.increase(1);
                if (writer != null) {
                    writer.println(value);
                    writer.flush();
                }
            }

        }).name("sink");

    }

}
