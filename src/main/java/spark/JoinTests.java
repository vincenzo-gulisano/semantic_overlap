package spark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

class TestUDF6 implements UDF6<Timestamp, Long, String, Timestamp, Long, String, Boolean> {

        private long threshold;

        private static ConcurrentHashMap<Long, CountStat> throughputStat;
        private static String throughputFile;

        public TestUDF6(long threshold, String outputFileRate) {
                System.out.println("Creating TestUDF6 instance");
                this.threshold = threshold;
                if (throughputStat == null) {

                        this.throughputFile = outputFileRate;
                        this.throughputStat = new ConcurrentHashMap<>();
                }
        }

        @Override
        public Boolean call(Timestamp ts1, Long t1, String t2, Timestamp ts2, Long t3, String t4)
                        throws Exception {
                long threadID = Thread.currentThread().getId();
                if (!throughputStat.containsKey(threadID)) {
                        String statFileName = throughputFile + "." + threadID;
                        throughputStat.put(threadID, new CountStat(statFileName, true));
                }
                throughputStat.get(threadID).increase(1);
                return t2.length() >= threshold && !t2.equals(t4) && Math.abs(t2.length() - t4.length()) < 2;
        }
}

public class JoinTests {

        public static void main(String[] args) throws IOException, TimeoutException, StreamingQueryException {

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

                        String throughputOutputFile = cmd.getOptionValue("to");
                        String latencyOutputFile = cmd.getOptionValue("lo");
                        long duration = Long.parseLong(cmd.getOptionValue("d"));
                        String experimentID = cmd.getOptionValue("eid");
                        String outputRateOutputFile = cmd.getOptionValue("ro");
                        int maxLatencyViolations = Integer.valueOf(cmd.getOptionValue("mlv"));
                        long maxLatency = Long.parseLong(cmd.getOptionValue("ml"));
                        ackFile = cmd.getOptionValue("ack");
                        long warmUp = Long.parseLong(cmd.getOptionValue("wu"));
                        String IP = cmd.getOptionValue("ip");
                        int port = Integer.valueOf(cmd.getOptionValue("p"));

                        SparkSession spark = SparkSession
                                        .builder()
                                        .appName("JoinTests")
                                        .config("spark.master", "local[*]")
                                        .getOrCreate();

                        spark.sparkContext().setLogLevel("ERROR");

                        Dataset<Row> left = spark.readStream()
                                        .format("socket")
                                        .option("host", IP)
                                        .option("port", port)
                                        .load();

                        // Split the 'value' column and cast to the specified data types
                        Dataset<Row> splitData = left
                                        .selectExpr(
                                                        "CAST(SPLIT(value, '\\t')[0] AS LONG) AS timestamp",
                                                        "CAST(SPLIT(value, '\\t')[1] AS STRING) AS orig",
                                                        "CAST(SPLIT(value, '\\t')[2] AS STRING) AS change",
                                                        "CAST(SPLIT(value, '\\t')[3] AS STRING) AS updated",
                                                        "CAST(SPLIT(value, '\\t')[0] AS LONG) AS stimulus");

                        Dataset<Row> splitDataET = splitData
                                        .withColumn("eventtime",
                                                        functions.to_timestamp(
                                                                        splitData.col("timestamp")
                                                                                        .divide(1000))
                                                                        .cast(DataTypes.TimestampType));

                        long WSSeconds = JoinParameters.getWS(ExperimentID.valueOf(experimentID));
                        Dataset<Row> splitDataW = splitDataET
                                        .withColumn("win",
                                                        functions.window(
                                                                        splitDataET.col("eventtime"),
                                                                        WSSeconds + " seconds"));

                        spark.udf().register("comparisonUDF",
                                        new TestUDF6(JoinParameters.getMinLength(ExperimentID.valueOf(experimentID)),
                                                        throughputOutputFile),
                                        DataTypes.BooleanType);
                        
                        // Join with event-time constraints
                        // NOTICE: Stream-stream join without equality predicate is not supported.
                        Dataset<Row> joinResult = splitDataW.as("left")
                                        .join(
                                                        splitDataW.as("right"),
                                                        functions.expr("left.win.start=right.win.start AND size(split(left.change, ' '))==size(split(right.change, ' '))"
                                                                        + " AND comparisonUDF(left.eventtime,left.timestamp,left.orig,right.eventtime,right.timestamp,right.orig)"));

                        StreamingQuery query = joinResult
                                        .writeStream()
                                        // .format("console")
                                        .trigger(Trigger.ProcessingTime(200))
                                        .foreach(new MySinkRowJoin(
                                                        latencyOutputFile,
                                                        outputRateOutputFile, maxLatency, maxLatencyViolations))

                                        .start();

                        query.awaitTermination((long) (duration * 1.2 + warmUp));
                        
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
