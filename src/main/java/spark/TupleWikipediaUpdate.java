package spark;

public class TupleWikipediaUpdate {

    private final Long timestamp;
    private final String orig;
    private final String change;
    private final String updated;
    private final Long stimulus;

    public TupleWikipediaUpdate(long timestamp, String orig, String change, String updated, long stimulus) {
        this.timestamp = timestamp;
        this.orig = orig;
        this.change = change;
        this.updated = updated;
        this.stimulus = stimulus;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getOrig() {
        return orig;
    }

    public String getChange() {
        return change;
    }

    public String getUpdated() {
        return updated;
    }

    public Long getStimulus() {
        return stimulus;
    }

    @Override
    public String toString() {
        return orig + "," + change + ","
                + updated;
    }

}
