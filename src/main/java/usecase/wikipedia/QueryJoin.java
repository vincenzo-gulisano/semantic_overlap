package usecase.wikipedia;

import java.io.Serializable;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import usecase.ExperimentID;
import util.CountStat;

class TupleMatcherSPEExp implements Serializable {

        public boolean match(TupleWikipediaUpdate t1, TupleWikipediaUpdate t2, ExperimentID expID) {
                return !t1.f1.equals(t2.f1) && t1.f1.length() >= JoinParameters.getMinLength(expID)
                                && Math.abs(t1.f1.length()
                                                - t2.f1.length()) < 2;
        }

}

public class QueryJoin {

        static CountStat throughputStat;

        public static void main(String[] args) throws Exception {

                // Managing parameters (here and only here)
                ParameterTool parameter = ParameterTool.fromArgs(args);
                String inputFile1 = parameter.get("inputFile");
                int allowedLateness = parameter.getInt("allowedLateness", Integer.MAX_VALUE);
                ExperimentID expID = ExperimentID.valueOf(parameter.get("experimentID"));

                int maxLatency = parameter.getInt("maxLatency", 20000);
                int maxLatencyViolations = parameter.getInt("maxLatencyViolations", 10);

                // The following pararmeters are option and used for performance experiments
                String injectionRateOutputFile1 = parameter.get("injectionRateOutputFile");
                String throughputOutputFile1 = parameter.get("throughputOutputFile");
                String outputFile = parameter.get("outputFile", "");
                String latencyOutputFile = parameter.get("latencyOutputFile");
                String outputRateOutputFile = parameter.get("outputRateOutputFile");
                String statsFolder = parameter.get("statsFolder");
                long duration = parameter.getLong("duration");
                int batchSize = parameter.getInt("batchSize");
                long sleepTime = parameter.getLong("sleepTime");

                // Environment setup
                StreamExecutionEnvironment env = StreamExecutionEnvironment
                                .getExecutionEnvironment();
                env.setParallelism(1);

                SingleOutputStreamOperator<TupleWikipediaUpdate> sourceStream1 = SourceWikipediaUpdate
                                .createTimestampStringTupleSourceForPerformanceStudy(env,
                                                inputFile1, injectionRateOutputFile1, allowedLateness, duration,
                                                batchSize, sleepTime)
                                .name("source");

                // Make sure a chain starts here
                sourceStream1.startNewChain();

                DataStream<TupleWikipediaUpdate> joinStream = null;

                joinStream = sourceStream1.join(sourceStream1).where(t -> t.f2.split(" ").length)
                                .equalTo(t -> t.f2.split(" ").length)
                                .window(TumblingEventTimeWindows.of(
                                                Time.milliseconds(JoinParameters.getWS(expID))))
                                .apply(new FlatJoinFunction<TupleWikipediaUpdate, TupleWikipediaUpdate, TupleWikipediaUpdate>() {

                                        TupleMatcherSPEExp tm = new TupleMatcherSPEExp();

                                        @Override
                                        public void join(TupleWikipediaUpdate first,
                                                        TupleWikipediaUpdate second,
                                                        Collector<TupleWikipediaUpdate> out)
                                                        throws Exception {

                                                QueryJoin.throughputStat.increase(1);
                                                if (tm.match(first, second, expID)) {

                                                        out.collect(new TupleWikipediaUpdate(
                                                                        Math.max(first.f0,
                                                                                        second.f0),
                                                                        first.f1,
                                                                        second.f1,
                                                                        "",
                                                                        Math.max(first.f4,
                                                                                        second.f4)));

                                                }
                                        }

                                });

                SinkLatencyRateLogger.createTimestampStringTupleSinkPerformance(joinStream, latencyOutputFile,
                                outputRateOutputFile, maxLatency, maxLatencyViolations, outputFile, statsFolder);

                QueryJoin.throughputStat = new CountStat(throughputOutputFile1, true);

                env.execute();

        }
}
