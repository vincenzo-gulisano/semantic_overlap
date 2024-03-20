package muSPE;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import common.util.Util;
import component.operator.in1.aggregate.Aggregate;
import component.sink.Sink;
import component.sink.SinkFunction;
import component.source.Source;
import query.Query;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

class TupleMatcher implements Function<Queue<TupleWikipediaUpdate>, Queue<TupleWikipediaUpdate>> {

        private final ExperimentID id;

        private final CountStat comparisonsRateStat;

        public TupleMatcher(ExperimentID id, String comparisonsRateStatFile) {
                this.id = id;
                this.comparisonsRateStat = new CountStat(comparisonsRateStatFile, false);
        }

        @Override
        public Queue<TupleWikipediaUpdate> apply(Queue<TupleWikipediaUpdate> ts) {
                Queue<TupleWikipediaUpdate> result = new LinkedList<>();
                for (TupleWikipediaUpdate t1 : ts) {
                        for (TupleWikipediaUpdate t2 : ts) {
                                if (t1.getOrig().length() >= JoinParameters.getMinLength(id)) {
                                        if (!t1.getOrig().equals(t2.getOrig()) && Math.abs(
                                                        t1.getOrig().length() - t2.getOrig().length()) < 2) {
                                                result.add(new TupleWikipediaUpdate(
                                                                Math.max(t1.getTimestamp(),
                                                                                t2.getTimestamp()),
                                                                t1.getOrig(),
                                                                t2.getOrig(),
                                                                "",
                                                                Math.max(t1.getStimulus(),
                                                                                t2.getStimulus()),
                                                                t1.getUniqueID() * t2.getUniqueID()));
                                        }
                                }
                        }
                }
                this.comparisonsRateStat.increase(ts.size() * ts.size());
                return result;
        }

        public void close() {
                this.comparisonsRateStat.close();
        }

}

public class JoinQuery {

        public static void main(String[] args) throws IOException, TimeoutException {

                Options options = new Options();
                options.addOption("i", "inputFile", true, "Input file path");
                options.addOption("o", "outputFile", true, "Output File");
                options.addOption("p", "port", true, "port");
                options.addOption("ip", "IP", true, "IP");
                options.addOption("ir", "injectionRateOutputFile", true, "Injection rate output file");
                options.addOption("to", "throughputOutputFile", true, "Throughput output file");
                options.addOption("lo", "latencyOutputFile", true, "Latency output file");
                options.addOption("d", "duration", true, "Duration in seconds");
                options.addOption("st", "sleepTime", true, "Sleep time in milliseconds");
                options.addOption("eid", "experimentID", true, "Experiment ID");
                options.addOption("ro", "outputRateOutputFile", true, "Output rate output file");
                options.addOption("mlv", "maxLatencyViolations", true, "Max latency violations");
                options.addOption("ml", "maxLatency", true, "Max latency");
                options.addOption("ack", "ack", true, "Ack file");
                options.addOption("wu", "warmup", true, "warmup");

                CommandLineParser parser = new GnuParser();

                String ackFile = "";

                try {
                        CommandLine cmd = parser.parse(options, args);

                        String inputFile = cmd.getOptionValue("i");
                        String outputFile = cmd.getOptionValue("o", "");
                        String injectionRateOutputFile = cmd.getOptionValue("ir");
                        String throughputOutputFile = cmd.getOptionValue("to");
                        String latencyOutputFile = cmd.getOptionValue("lo");
                        long duration = Long.parseLong(cmd.getOptionValue("d"));
                        long sleepTime = Long.parseLong(cmd.getOptionValue("st"));
                        String experimentID = cmd.getOptionValue("eid");
                        String outputRateOutputFile = cmd.getOptionValue("ro");
                        int maxLatencyViolations = Integer.valueOf(cmd.getOptionValue("mlv"));
                        long maxLatency = Long.parseLong(cmd.getOptionValue("ml"));
                        ackFile = cmd.getOptionValue("ack");

                        long rate = 1000000000L / sleepTime;
                        System.out.println("trying rate " + rate);

                        Query q = new Query();

                        Source<TupleWikipediaUpdate> s = q.addBaseSource("in",
                                        new SourceReadFromFile(inputFile, injectionRateOutputFile, throughputOutputFile,
                                                        duration,
                                                        sleepTime));
                        TupleMatcher tm = new TupleMatcher(ExperimentID
                                        .valueOf(experimentID), throughputOutputFile);
                        Aggregate<TupleWikipediaUpdate, TupleWikipediaUpdate, Long> agg = new Aggregate<>(
                                        "agg", 0, 1, JoinParameters.getWS(ExperimentID
                                                        .valueOf(experimentID)),
                                        JoinParameters.getWS(ExperimentID
                                                        .valueOf(experimentID)),
                                        tm,
                                        t -> (long) t.getChange().split(" ").length,
                                        t -> t.getTimestamp());
                        q.addOperator(agg);

                        Sink<TupleWikipediaUpdate> o1 = q
                                        .addSink(new MySinkRow("out", new SinkFunction<TupleWikipediaUpdate>() {

                                                @Override
                                                public void accept(TupleWikipediaUpdate t) {
                                                }

                                        }, latencyOutputFile,
                                                        outputRateOutputFile, outputFile, maxLatency,
                                                        maxLatencyViolations));

                        q.connect(s, agg).connect(agg, o1);

                        q.activate();
                        Util.sleep((long) (duration * 1.1));
                        q.deActivate();

                        tm.close();

                } catch (ParseException e) {
                        System.err.println("Error parsing command line options: " + e.getMessage());
                        System.exit(1);
                }

                // Create the empty file
                File emptyFile = new File(ackFile);
                try {
                        emptyFile.createNewFile();
                } catch (IOException e) {
                        System.err.println("An error occurred: " + e.getMessage());
                }

        }

}
