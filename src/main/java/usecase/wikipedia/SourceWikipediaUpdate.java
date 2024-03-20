package usecase.wikipedia;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;

import util.CountStat;

public class SourceWikipediaUpdate {

    public static SingleOutputStreamOperator<TupleWikipediaUpdate> createTimestampStringTupleSourceForPerformanceStudy(
            StreamExecutionEnvironment env, String inputFile, String statFile, long maxAllowedLateness, long duration,
            int batchSize, long sleepPeriod) {
        return createTimestampStringTupleSourceForPerformanceStudy(env, inputFile, statFile, maxAllowedLateness,
                duration, batchSize, sleepPeriod, 10 * 1000000000L, 1);
    }

    public static SingleOutputStreamOperator<TupleWikipediaUpdate> createTimestampStringTupleSourceForPerformanceStudy(
            StreamExecutionEnvironment env, String inputFile, String statFile, long maxAllowedLateness, long duration,
            int batchSize, long sleepPeriod, long maxIdleTime, int maxIdleTimeOccurences) {

        return env.addSource(new RichSourceFunction<TupleWikipediaUpdate>() {

            private CountStat log;
            private ConcurrentLinkedQueue<TupleWikipediaUpdate> internalQueue;
            private boolean internalThreadCompleted = false;

            @Override
            public void open(Configuration parameters) throws Exception {
                log = new CountStat(statFile, true);
                internalQueue = new ConcurrentLinkedQueue<>();
            }

            @Override
            public void close() throws Exception {
                log.close();
            }

            private void fillInternalQueue(String inputFile) throws IOException {

                ArrayList<String[]> internalBuffer = new ArrayList<>();

                BufferedReader inputFileReader = new BufferedReader(new FileReader(inputFile));
                String line = inputFileReader.readLine();
                while (line != null && internalBuffer.size() < 10000) {
                    internalBuffer.add(line.split("\\t"));
                    line = inputFileReader.readLine();
                }
                inputFileReader.close();
                int internalBufferIdx = 0;

                long startTime = System.currentTimeMillis();

                String[] lineTokens;
                long ts;

                while (System.currentTimeMillis() - startTime <= duration) {

                    long beforeBatchTime = System.nanoTime();
                    for (int j = 0; j < batchSize; j++) {

                        lineTokens = internalBuffer.get(internalBufferIdx);
                        ts = System.currentTimeMillis();
                        internalBufferIdx = (internalBufferIdx + 1) % internalBuffer.size();
                        internalQueue.add(new TupleWikipediaUpdate(ts,
                                lineTokens[0], lineTokens[1],
                                lineTokens[2], ts));
                        log.increase(1);
                    }

                    while (System.nanoTime() < beforeBatchTime + sleepPeriod) {
                    }

                }
                internalThreadCompleted = true;

            }

            @Override
            public void run(SourceContext<TupleWikipediaUpdate> ctx) throws Exception {

                new Thread(() -> {
                    try {
                        fillInternalQueue(inputFile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }).start();

                while (!internalThreadCompleted) {
                    if (!internalQueue.isEmpty()) {
                        ctx.collect(internalQueue.poll());
                    }
                }

            }

            @Override
            public void cancel() {
            }

        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<TupleWikipediaUpdate>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.f0));

    }

}
