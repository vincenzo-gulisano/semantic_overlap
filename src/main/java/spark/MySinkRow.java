package spark;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.sql.*;

public class MySinkRow extends ForeachWriter<Row> {

    private static ConcurrentHashMap<Long, AvgStat> logLatency;
    private static ConcurrentHashMap<Long, CountStat> logRate;
    private String outputFileLatency;
    private String outputFileRate;
    private static HashSet<Long> threadsIDs;
    private static AtomicInteger observedViolations;
    long maxLatencyValue;
    int latencyViolationMaxOccurences;

    public MySinkRow(String outputFileLatency, String outputFileRate, long maxLatencyValue,
            int latencyViolationMaxOccurences) {
        this.outputFileLatency = outputFileLatency;
        this.outputFileRate = outputFileRate;
        this.maxLatencyValue = maxLatencyValue;
        this.latencyViolationMaxOccurences = latencyViolationMaxOccurences;
    }

    @Override
    public void close(Throwable errorOrNull) {
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        return true;
    }

    @Override
    public void process(Row t) {

        long threadID = Thread.currentThread().getId();
        if (logLatency == null || logRate == null || threadsIDs == null) {
            logLatency = new ConcurrentHashMap<>();
            logRate = new ConcurrentHashMap<>();
            threadsIDs = new HashSet<>();
            observedViolations = new AtomicInteger(0);
        }
        if (!threadsIDs.contains(threadID)) {
            threadsIDs.add(threadID);
        }
        if (!logLatency.containsKey(threadID)) {
            String statFileName = outputFileLatency + "." + threadID;
            logLatency.put(threadID, new AvgStat(statFileName, true));
        }
        if (!logRate.containsKey(threadID)) {
            String statFileName = outputFileRate + "." + threadID;
            logRate.put(threadID, new CountStat(statFileName, true));
        }

        if (logLatency.get(threadID).add(System.currentTimeMillis() - t.getLong(4)) >= maxLatencyValue) {
            System.out.println(
                    "Observed violations: " + observedViolations.get() + "->" + (observedViolations.get() + 1));
            observedViolations.incrementAndGet();
            if (observedViolations.get() >= latencyViolationMaxOccurences) {
                System.out.println("Max latency violated");
                System.out.println("The sink has observed the max latency " + maxLatencyValue
                        + " being violated at least " + observedViolations + " times! Terminating!");
                System.exit(13);
            }
        }
        logRate.get(threadID).increase(1);
    }

}
