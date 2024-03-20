package usecase.synthetic;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import usecase.wikipedia.SinkLatencyRateLogger;
import util.ImplementationType;
import util.Instrumenter;

public class QueryFlatMap {
        public static void main(String[] args) throws Exception {

                // Managing parameters (here and only here)
                ParameterTool parameter = ParameterTool.fromArgs(args);
                int allowedLateness = parameter.getInt("allowedLateness", Integer.MAX_VALUE);
                ImplementationType implementationType = ImplementationType.valueOf(parameter.get("implementationType"));

                int maxLatency = parameter.getInt("maxLatency", 20000);
                int maxLatencyViolations = parameter.getInt("maxLatencyViolations", 10);

                // The following pararmeters are option and used for performance experiments
                String injectionRateOutputFile = parameter.get("injectionRateOutputFile");
                String throughputOutputFile = parameter.get("throughputOutputFile");
                String latencyOutputFile = parameter.get("latencyOutputFile");
                String outputRateOutputFile = parameter.get("outputRateOutputFile");
                String statsFolder = parameter.get("statsFolder");
                long duration = parameter.getLong("duration");
                long sleepTime = parameter.getLong("sleepTime");

                long workloadIterations = parameter.getLong("workloadIterations");
                double selectivity = parameter.getDouble("selectivity");

                // Environment setup
                StreamExecutionEnvironment env = StreamExecutionEnvironment
                                .getExecutionEnvironment();
                env.setParallelism(1);

                Instrumenter inst = new Instrumenter();

                SingleOutputStreamOperator<InputTuple> sourceStream = Source
                                .createTimestampStringTupleSourceForPerformanceStudy(env, injectionRateOutputFile,
                                                allowedLateness, duration, sleepTime)
                                .name("source");

                sourceStream = inst.addThroughputLogger(sourceStream, throughputOutputFile).name("throughput-logger");

                // Make sure a chain starts here
                sourceStream.startNewChain();

                // Instantiate the FlatMap
                SingleOutputStreamOperator<InputTuple> flatMapStream = null;
                switch (implementationType) {
                        case SINGLEOUT:
                                flatMapStream = inst.addAggBasedFlatMapSingleOut(
                                                sourceStream,
                                                FlatMapFunctions.mapAggBasedSingleOut(workloadIterations, selectivity),
                                                Time.milliseconds(allowedLateness),
                                                new TypeFactoryInputTuple()).name("agg-based-FM-singleout");
                                break;
                        case MULTIOUT:
                                flatMapStream = inst.addAggBasedFlatMapMultiOut(sourceStream,
                                                FlatMapFunctions.mapAggBasedMultiOut(workloadIterations, selectivity),
                                                Time.milliseconds(allowedLateness),
                                                new TypeFactoryInputTuple()).name("agg-based-FM-multiout");
                                break;
                        case NATIVE:
                                flatMapStream = sourceStream
                                                .flatMap(FlatMapFunctions.mapNative(workloadIterations, selectivity))
                                                .name("native-FM");
                                break;
                        default:
                                throw new RuntimeException("Unkown type");
                }

                // Make sure a chain starts here
                flatMapStream.startNewChain();

                // Instantiate the sink (the same for native and aggregate-based)
                SinkLatencyRateLogger.createTimestampStringTupleSinkPerformance(flatMapStream, latencyOutputFile,
                                outputRateOutputFile, maxLatency, maxLatencyViolations, "", statsFolder);

                env.execute();

        }
}
