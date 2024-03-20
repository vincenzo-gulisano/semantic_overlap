package spark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class FlatMapCSVRow {

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

                CommandLineParser parser = new GnuParser();

                String ackFile = "";

                try {
                        CommandLine cmd = parser.parse(options, args);

                        String inputFile = cmd.getOptionValue("i");
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

                        SparkSession spark = SparkSession
                                        .builder()
                                        .appName("JavaStructuredNetworkWordCount")
                                        .config("spark.master", "local[*]")
                                        .getOrCreate();

                        long rate = 1000000000L / sleepTime;
                        System.out.println("trying rate " + rate);

                        System.out.println("initializing source function with " + inputFile + " and "
                                        + injectionRateOutputFile);

                        // Define your custom row-generating function
                        Dataset<Row> lines = spark.readStream()
                                        .format("rate") // Use a rate source for generating rows
                                        .option("rowsPerSecond", rate) // Set the desired rate (adjust as needed)
                                        .load()
                                        .selectExpr("value as id") // Rename the value column to "id"
                                        .map(new SourceFunction(), Encoders.STRING()) // Apply
                                                                                      // the
                                                                                      // custom
                                                                                      // function
                                        .withColumnRenamed("value", "value");

                        // Split the 'value' column and cast to the specified data types
                        Dataset<Row> splitData = lines
                                        .selectExpr(
                                                        "CAST(SPLIT(value, '\\t')[0] AS LONG) AS timestamp",
                                                        "CAST(SPLIT(value, '\\t')[1] AS STRING) AS a",
                                                        "CAST(SPLIT(value, '\\t')[2] AS STRING) AS b",
                                                        "CAST(SPLIT(value, '\\t')[3] AS STRING) AS c",
                                                        "CAST(SPLIT(value, '\\t')[0] AS LONG) AS stimulus");

                        spark.sparkContext().setLogLevel("ERROR");

                        SourceFunction.open(inputFile, injectionRateOutputFile, throughputOutputFile, duration, 1,
                                        sleepTime);

                        // Start running the query that prints the running counts to the console
                        StreamingQuery query = splitData
                                       .flatMap(FlatMapFunctions.instantiateFlatMapFunctionNativeRow(
                                                        ExperimentID.valueOf(experimentID)), Encoders.kryo(Row.class))
                                        .writeStream()
                                        .foreach(new MySinkRow(
                                                        latencyOutputFile,
                                                        outputRateOutputFile, maxLatency, maxLatencyViolations))
                                        .start();

                        query.awaitTermination((long) (duration * 1.2));
                        SourceFunction.close();

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
