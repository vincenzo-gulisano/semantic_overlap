package usecase.synthetic;

import org.apache.flink.util.Collector;

import util.Instrumenter.SerializableProcessFunction;

public class LoopPerformance {

    public static void main(String[] args) {
        runLoop(1000);
        runLoop(5000);
        runLoop(10000);
        runLoop(20000);
        runLoop(40000);
        runLoop(100000);
    }

    private static void runLoop(int iterations) {
        System.out.println(
                "Average Elapsed Time for " + iterations + " Iterations: " + calculateAverageTime(iterations) + " ms");
        System.out.println("------------------------------");
    }

    private static long calculateAverageTime(int iterations) {

        InputTuple t = new InputTuple(0L,0,0L);
        Collector<InputTuple> out = new Collector<InputTuple>() {

            @Override
            public void collect(InputTuple record) {
                
            }

            @Override
            public void close() {
                
            }
            
        };
        SerializableProcessFunction<InputTuple, InputTuple> f = FlatMapFunctions.mapAggBasedMultiOut(iterations, 1);

        long totalTime = 0;
        long reps = 1000000;
        // Calculate the total time from all iterations
        for (long i = 0; i < reps; i++) {
            long startTime = System.nanoTime();
            f.process(t , out);
            
            long endTime = System.nanoTime();
            totalTime += endTime - startTime;
        }

        // Return the average time
        return totalTime / reps;
    }
}
