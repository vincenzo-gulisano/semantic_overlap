package muSPE;

public class TupleWikipediaUpdate {

    private final Long timestamp;
    private final String orig;
    private final String change;
    private final String updated;
    private final Long stimulus;
    private final long uniqueID;

    public TupleWikipediaUpdate(long timestamp, String orig, String change, String updated,
            long uniqueID) {
        this(timestamp, orig, change, updated, timestamp, uniqueID);
    }

    public TupleWikipediaUpdate(long timestamp, String orig, String change, String updated, long stimulus,
            long uniqueID) {
        this.timestamp = timestamp;
        this.orig = orig;
        this.change = change;
        this.updated = updated;
        this.stimulus = stimulus;
        this.uniqueID = uniqueID;
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

    public long getUniqueID() {
        return uniqueID;
    }

}
