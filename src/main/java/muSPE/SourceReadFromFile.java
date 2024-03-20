package muSPE;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import component.source.SourceFunction;

public class SourceReadFromFile implements SourceFunction<TupleWikipediaUpdate> {

    private CountStat injectionRateStat;
    private CountStat throughputStat;

    private String inputFilePath;
    private long duration;
    private long sleepPeriod;
    private volatile boolean enabled;

    private ConcurrentLinkedQueue<TupleWikipediaUpdate> internalQueue;
    private boolean internalThreadCompleted = false;

    private long tupleID;

    public SourceReadFromFile(String inputFilePath, String injFile, String thrFile, long duration, long sleepPeriod) {
        injectionRateStat = new CountStat(injFile, true);
        throughputStat = new CountStat(thrFile, true);
        internalQueue = new ConcurrentLinkedQueue<>();
        this.inputFilePath = inputFilePath;
        this.duration = duration;
        this.sleepPeriod = sleepPeriod;
        this.tupleID = 0;
    }

    private void fillInternalQueue()
            throws IOException {

        System.out.println("Starting fillInternalQueue");

        ArrayList<String[]> internalBuffer = new ArrayList<>();
        int internalBufferIdx = 0;

        BufferedReader inputFileReader = new BufferedReader(new FileReader(inputFilePath));

        String line = inputFileReader.readLine();
        while (line != null && internalBuffer.size() < 10000) {
            internalBuffer.add(line.split("\\t"));
            line = inputFileReader.readLine();
        }
        inputFileReader.close();

        long startTime = System.nanoTime();
        long durationInNanoTime = duration * 1000000L;

        String[] lineTokens;
        long beforeBatchTime = System.nanoTime();
        while (!internalThreadCompleted && enabled && beforeBatchTime - startTime <= durationInNanoTime) {

            lineTokens = internalBuffer.get(internalBufferIdx);
            internalBufferIdx = (internalBufferIdx + 1) % internalBuffer.size();

            tupleID++;
            internalQueue.add(new TupleWikipediaUpdate(System.currentTimeMillis(),
                    lineTokens[0], lineTokens[1],
                    lineTokens[2], tupleID));
            injectionRateStat.increase(1);

            while (System.nanoTime() < beforeBatchTime + sleepPeriod) {
            }
            beforeBatchTime = System.nanoTime();
        }
        internalThreadCompleted = true;
        System.out.println("exiting fillInternalQueue");

    }

    @Override
    public TupleWikipediaUpdate get() {

        while (true) {
            while (enabled && internalQueue.isEmpty()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    System.out.println("Sleep in source was interrupted");
                }
            }
            TupleWikipediaUpdate tuple = internalQueue.poll();
            if (tuple != null) {
                throughputStat.increase(1);
                return tuple;
            } else {
                return null;
            }
        }

    }

    @Override
    public boolean isInputFinished() {
        return internalThreadCompleted && internalQueue.isEmpty();
    }

    @Override
    public void enable() {
        this.enabled = true;
        new Thread(() -> {
            try {
                fillInternalQueue();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void disable() {
        this.enabled = false;
    }

    @Override
    public boolean canRun() {
        return !isInputFinished();
    }

}
