package usecase.synthetic;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;

import util.CountStat;

public class Source {

    public static SingleOutputStreamOperator<InputTuple> createTimestampStringTupleSourceForPerformanceStudy(
            StreamExecutionEnvironment env, String statFile, long maxAllowedLateness, long duration,
            long sleepPeriod) {
        return createTimestampStringTupleSourceForPerformanceStudy(env, statFile, maxAllowedLateness,
                duration, sleepPeriod, 10 * 1000000000L, 1);
    }

    public static SingleOutputStreamOperator<InputTuple> createTimestampStringTupleSourceForPerformanceStudy(
            StreamExecutionEnvironment env, String statFile, long maxAllowedLateness, long duration,
            long sleepPeriod, long maxIdleTime, int maxIdleTimeOccurences) {

        return env.addSource(new RichSourceFunction<InputTuple>() {

            private CountStat log;
            private int tupleID;

            @Override
            public void open(Configuration parameters) throws Exception {
                log = new CountStat(statFile, true);
                tupleID = 0;
            }

            @Override
            public void close() throws Exception {
                log.close();
            }

            @Override
            public void run(SourceContext<InputTuple> ctx) throws Exception {

                long startTime = System.currentTimeMillis();
                while (System.currentTimeMillis() - startTime <= duration) {
                    long beforeSendTime = System.nanoTime();
                    long ts = System.currentTimeMillis();
                    ctx.collect(new InputTuple(ts, tupleID, ts));
                    log.increase(1);
                    tupleID++;
                    while (System.nanoTime() < beforeSendTime + sleepPeriod) {
                    }
                }

            }

            @Override
            public void cancel() {
            }

        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<InputTuple>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.f0));

    }

}
