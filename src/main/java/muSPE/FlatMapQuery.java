package muSPE;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import common.util.Util;
import component.operator.Operator;
import component.operator.in1.aggregate.Aggregate;
import component.sink.Sink;
import component.sink.SinkFunction;
import component.source.Source;
import query.Query;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class FlatMapQuery {

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
                // options.addOption("bs", "batchSize", true, "Batch size");
                options.addOption("st", "sleepTime", true, "Sleep time in milliseconds");
                options.addOption("eid", "experimentID", true, "Experiment ID");
                options.addOption("ro", "outputRateOutputFile", true, "Output rate output file");
                options.addOption("mlv", "maxLatencyViolations", true, "Max latency violations");
                options.addOption("ml", "maxLatency", true, "Max latency");
                options.addOption("ack", "ack", true, "Ack file");
                options.addOption("parallelism", "parallelism", true, "parallelism");

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

                        int parallelism = Integer.parseInt(cmd.getOptionValue("parallelism"));

                        long rate = 1000000000L / sleepTime;
                        System.out.println("trying rate " + rate);

                        // SourceFunction sourceFunction = new SourceFunction();
                        System.out.println("initializing source function with " + inputFile + " and "
                                        + injectionRateOutputFile);

                        Query q = new Query();

                        Source<TupleWikipediaUpdate> s = q.addBaseSource("in",
                                        new SourceReadFromFile(inputFile, injectionRateOutputFile, throughputOutputFile,
                                                        duration,
                                                        sleepTime));

                        List<Aggregate<TupleWikipediaUpdate, TupleWikipediaUpdate, Long>> aggs = new LinkedList<>();
                        for (int p = 0; p < parallelism; p++) {

                                Aggregate<TupleWikipediaUpdate, TupleWikipediaUpdate, Long> agg = new Aggregate<>(
                                                "agg" + p, p, parallelism, 1, 1,
                                                FlatMapFunctions.instantiateFlatMapFunctionNative(
                                                                ExperimentID.valueOf(experimentID)),
                                                t -> t.getUniqueID(),
                                                t -> t.getTimestamp());
                                aggs.add(agg);
                                q.addOperator(agg);
                        }

                        MyRouter<TupleWikipediaUpdate> r = new MyRouter<TupleWikipediaUpdate>("r",
                                        t -> t.getUniqueID());
                        q.addOperator(r);

                        Operator<TupleWikipediaUpdate, TupleWikipediaUpdate> o1 = q
                                        .addOperator(new MySinkRowOperator("out",
                                                        new SinkFunction<TupleWikipediaUpdate>() {

                                                                @Override
                                                                public void accept(TupleWikipediaUpdate t) {
                                                                }

                                                        }, latencyOutputFile,
                                                        outputRateOutputFile, outputFile, maxLatency,
                                                        maxLatencyViolations, duration));

                        q.connect(s, r);
                        for (int p = 0; p < parallelism; p++) {
                                q.connect(r, aggs.get(p));
                                q.connect(aggs.get(p), o1);
                        }

                        // This is an ack because muSPE wants all operators to have at least 1 output
                        Sink<TupleWikipediaUpdate> sink = q.addBaseSink("ack",
                                        new SinkFunction<TupleWikipediaUpdate>() {

                                                @Override
                                                public void accept(TupleWikipediaUpdate arg0) {
                                                }

                                        });
                        q.connect(o1, sink);

                        q.activate();
                        Util.sleep((long) (duration * 1.1));
                        q.deActivate();

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
