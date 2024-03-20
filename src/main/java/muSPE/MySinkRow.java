package muSPE;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import component.sink.BaseSink;
import component.sink.SinkFunction;

public class MySinkRow extends BaseSink<TupleWikipediaUpdate> {

    private AvgStat logLatency;
    private CountStat logRate;
    private String outputFile;
    private long observedViolations;
    private int latencyViolationMaxOccurences;
    private long maxLatencyValue;
    private PrintWriter writer;

    public MySinkRow(String id, SinkFunction<TupleWikipediaUpdate> function, String outputFileLatency,
            String outputFileRate, String outputFile, long maxLatencyValue,
            int latencyViolationMaxOccurences) {
        super(id, function);
        this.logLatency = new AvgStat(outputFileLatency, true);
        this.logRate = new CountStat(outputFileRate, true);

        this.maxLatencyValue = maxLatencyValue;
        this.latencyViolationMaxOccurences = latencyViolationMaxOccurences;
        this.outputFile = outputFile;
    }

    @Override
    public void enable() {
        super.enable();
        if (!outputFile.equals("")) {
            try {
                this.writer = new PrintWriter(new FileWriter(outputFile), true);
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format("Cannot write to file :%s", outputFile));
            }
        }
    }

    @Override
    public void disable() {
        super.disable();
        if (!outputFile.equals("")) {
            writer.flush();
            writer.close();
        }
    }

    @Override
    public void processTuple(TupleWikipediaUpdate t) {
        super.processTuple(t);

        if (logLatency.add(System.currentTimeMillis() - t.getStimulus()) >= maxLatencyValue) {
            System.out.println(
                    "Observed violations: " + observedViolations + "->" + (observedViolations + 1));
            observedViolations++;
            if (observedViolations >= latencyViolationMaxOccurences) {
                System.out.println("Max latency violated");
                System.out.println("The sink has observed the max latency " + maxLatencyValue
                        + " being violated at least " + observedViolations + " times! Terminating!");
                System.exit(13);
            }
        }
        if (!outputFile.equals("")) {
            writer.println(t);
        }
        logRate.increase(1);

    }

}