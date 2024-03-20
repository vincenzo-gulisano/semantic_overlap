package usecase.synthetic;

import org.apache.flink.api.java.tuple.Tuple3;
import util.TimestampedTuple;

public class InputTuple extends Tuple3<Long, Integer, Long> implements TimestampedTuple {

    public InputTuple() {
    }

    public InputTuple(Long f0, Integer f1, Long f2) {
        super(f0, f1, f2);
    }

    @Override
    public long getTimestamp() {
        return f0;
    }

    @Override
    public void setTimestamp(long timestamp) {
        f0 = timestamp;
    }

    public int getID() {
        return f1;
    }

    public void setID(int id) {
        f1 = id;
    }

    @Override
    public long getStimulus() {
        return f2;
    }

    @Override
    public void setStimulus(long stimulus) {
        f2 = stimulus;
    }

    @Override
    public String toString() {
        return f0 + "," + f1 + "," + f2;
    }

}
