package muSPE;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import component.ComponentType;
import component.operator.AbstractOperator;
import component.sink.SinkFunction;
import stream.Stream;

public class MySinkRowOperator extends AbstractOperator<TupleWikipediaUpdate, TupleWikipediaUpdate> {

    private AvgStat logLatency;
    private CountStat logRate;
    private String outputFile;
    private long observedViolations;
    private int latencyViolationMaxOccurences;
    private long maxLatencyValue;
    private PrintWriter writer;
    private long startTimeMilliseconds;
    private long durationMilliseconds;

    public MySinkRowOperator(String id, SinkFunction<TupleWikipediaUpdate> function, String outputFileLatency,
            String outputFileRate, String outputFile, long maxLatencyValue,
            int latencyViolationMaxOccurences, long durationMilliseconds) {
        super(id, ComponentType.UNION);
        this.logLatency = new AvgStat(outputFileLatency, true);
        this.logRate = new CountStat(outputFileRate, true);

        this.maxLatencyValue = maxLatencyValue;
        this.latencyViolationMaxOccurences = latencyViolationMaxOccurences;
        this.outputFile = outputFile;
        this.durationMilliseconds = durationMilliseconds;
    }

    @Override
    protected final void process() {
        if (isFlushed()) {
            return;
        }
        int finishedInputs = 0;
        for (Stream<TupleWikipediaUpdate> in : getInputs()) {
            TupleWikipediaUpdate inTuple = in.getNextTuple(getIndex());
            finishedInputs += isStreamFinished(inTuple, in) ? 1 : 0;
            if (inTuple != null) {
                processTuple(inTuple);
            }
        }
        if (finishedInputs == getInputs().size()) {
            flush();
            return;
        }
    }

    @Override
    public void addInput(Stream<TupleWikipediaUpdate> stream) {
        state.addInput(stream);
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
        startTimeMilliseconds = System.currentTimeMillis();
    }

    @Override
    public void disable() {
        super.disable();
        if (!outputFile.equals("")) {
            writer.flush();
            writer.close();
        }
    }

    public void processTuple(TupleWikipediaUpdate t) {
        
        if (logLatency.add(System.currentTimeMillis() - t.getStimulus()) >= maxLatencyValue) {
            System.out.println(
                    "Observed violations: " + observedViolations + "->" + (observedViolations + 1));
            observedViolations++;
            if (System.currentTimeMillis() - startTimeMilliseconds < durationMilliseconds) {
                if (observedViolations >= latencyViolationMaxOccurences) {
                    System.out.println("Max latency violated");
                    System.out.println("The sink has observed the max latency " + maxLatencyValue
                            + " being violated at least " + observedViolations + " times! Terminating!");
                    System.exit(13);
                }
            } else {
                System.out.println(
                        "Violations are happening after the experiment is completed, so I am assuming they are not important");
            }
        }
        if (!outputFile.equals("")) {
            writer.println(t);
        }
        logRate.increase(1);

    }

}