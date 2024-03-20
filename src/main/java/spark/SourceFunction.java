package spark;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class SourceFunction implements MapFunction<Row, String> {

    static public void open(String inputFilePath, String injFile, String thrFile, long duration,
            int batchSize, long sleepPeriod)
            throws FileNotFoundException {
        startTime = System.currentTimeMillis();
        injectionRateStat = new ConcurrentHashMap<>();
        throughputStat = new ConcurrentHashMap<>();
        internalQueue = new ConcurrentLinkedQueue<>();
        injectionRateFile = injFile;
        throughputFile = thrFile;

        new Thread(() -> {
            try {
                fillInternalQueue(inputFilePath, duration, batchSize, sleepPeriod);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

    }

    static public void close() throws IOException {
        internalThreadCompleted = true;
        for (CountStat cs : injectionRateStat.values()) {
            cs.close();
        }
        for (CountStat cs : throughputStat.values()) {
            cs.close();
        }
    }

    static long startTime;

    private static ConcurrentHashMap<Long, CountStat> injectionRateStat;
    private static ConcurrentHashMap<Long, CountStat> throughputStat;
    private static String injectionRateFile;
    private static String throughputFile;

    static String inputFilePath;

    static private ConcurrentLinkedQueue<String> internalQueue;
    static private boolean internalThreadCompleted = false;

    static private boolean firstCall = true;

    private static void fillInternalQueue(String inputFile, long duration, int batchSize, long sleepPeriod)
            throws IOException {

        long threadID = Thread.currentThread().getId();

        if (!injectionRateStat.containsKey(threadID)) {
            String statFileName = injectionRateFile + "." + threadID;
            injectionRateStat.put(threadID, new CountStat(statFileName, true));
        }
        CountStat thisRateStat = injectionRateStat.get(threadID);

        BufferedReader inputFileReader = new BufferedReader(new FileReader(inputFile));
        Random r = new Random();

        System.out.println("Injector filling in internal buffer...");
        int bufferSize = 10000;
        ArrayList<String> buffer = new ArrayList<>(bufferSize);
        String line = inputFileReader.readLine();
        while (line != null && buffer.size() < bufferSize) {
            buffer.add(line);
            line = inputFileReader.readLine();
        }
        inputFileReader.close();
        System.out.println("... buffer ready");

        long startTime = System.nanoTime();
        long durationInNanoTime = duration * 1000000L;

        long beforeBatchTime = System.nanoTime();
        while (!internalThreadCompleted && beforeBatchTime - startTime <= durationInNanoTime) {
            internalQueue.add(System.currentTimeMillis() + "\t" + buffer.get(r.nextInt(bufferSize)));
            thisRateStat.increase(1);

            while (System.nanoTime() < beforeBatchTime + sleepPeriod) {
            }

            beforeBatchTime = System.nanoTime();
        }
        internalThreadCompleted = true;

    }

    @Override
    public String call(Row value) throws Exception {

        if (firstCall) {
            firstCall = false;
            System.out.println("Time in between first call and internal thread start is: "
                    + (System.currentTimeMillis() - startTime) + " ms");
        }

        long threadID = Thread.currentThread().getId();
        if (!throughputStat.containsKey(threadID)) {
            String statFileName = throughputFile + "." + threadID;
            throughputStat.put(threadID, new CountStat(statFileName, true));
        }

        while (true) {
            while (internalQueue.isEmpty()) {
                Thread.sleep(1);
            }
            String tuple = internalQueue.poll();
            if (tuple != null) {
                throughputStat.get(threadID).increase(1);
                return tuple;
            }
        }
    }
}
