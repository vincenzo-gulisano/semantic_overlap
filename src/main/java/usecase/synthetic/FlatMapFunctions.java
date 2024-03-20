package usecase.synthetic;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import util.Instrumenter.SerializableProcessFunction;
import util.Instrumenter.SerializableLambdaFlatMap;

public class FlatMapFunctions {

    public static SerializableProcessFunction<InputTuple, InputTuple> mapAggBasedMultiOut(
            long workloadIterations, double selectivity) {
        return new SerializableProcessFunction<InputTuple, InputTuple>() {
            Random r = new Random();

            @Override
            public void process(InputTuple t, Collector<InputTuple> out) {
                int i = 0;
                for (; i < workloadIterations; i++) {

                }
                double j = selectivity;
                while (j > 0) {
                    if (r.nextDouble() < j) {
                        out.collect(t);
                    }
                    j -= 1.0;
                }
            }

        };
    }

    public static SerializableLambdaFlatMap<InputTuple, InputTuple> mapAggBasedSingleOut(
            long workloadIterations, double selectivity) {
        return new SerializableLambdaFlatMap<InputTuple, InputTuple>() {
            Random r = new Random();

            @Override
            public List<InputTuple> apply(InputTuple t) {
                List<InputTuple> output = new LinkedList<>();
                int i = 0;
                for (; i < workloadIterations; i++) {

                }
                double j = selectivity;
                while (j > 0) {
                    if (r.nextDouble() < j) {
                        output.add(t);
                    }
                    j -= 1.0;
                }
                return output;
            }

        };
    }

    public static FlatMapFunction<InputTuple, InputTuple> mapNative(
            long workloadIterations, double selectivity) {
        return new FlatMapFunction<InputTuple, InputTuple>() {
            Random r = new Random();

            @Override
            public void flatMap(InputTuple t, Collector<InputTuple> out) throws Exception {
                int i = 0;
                for (; i < workloadIterations; i++) {

                }
                double j = selectivity;
                while (j > 0) {
                    if (r.nextDouble() < j) {
                        out.collect(t);
                    }
                    j -= 1.0;
                }
            }

        };
    }

}
