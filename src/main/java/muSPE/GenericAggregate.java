// package muSPE;

// import java.util.HashMap;
// import java.util.LinkedList;
// import java.util.List;
// import java.util.Queue;
// import java.util.TreeMap;
// import java.util.Map.Entry;
// import java.util.function.Function;

// import component.operator.in1.BaseOperator1In;

// @SuppressWarnings("unchecked")
// public class GenericAggregate<IN, OUT, K> extends BaseOperator1In<IN, OUT> {

//     protected final int instance;
//     protected final int parallelismDegree;
//     protected final long WS;
//     protected final long WA;
//     private final long WS_WA_ceil;
//     private final long WS_WA_ceil_minus_1;
//     protected long latestTimestamp;
//     private boolean firstTuple = true;

//     public GenericAggregate(String id, int instance, int parallelismDegree, long ws, long wa,
//             Function<Queue<IN>, Queue<OUT>> aggF, Function<IN, K> getKey, Function<IN, Long> getTimestamp) {
//         super(id);
//         this.instance = instance;
//         this.parallelismDegree = parallelismDegree;
//         this.WS = ws;
//         this.WA = wa;
//         windows = new TreeMap<>();
//         this.aggF = aggF;
//         this.getKey = getKey;
//         this.getTimestamp = getTimestamp;
//         if (wa > ws) {
//             throw new AssertionError();
//         } else {
//             this.WS_WA_ceil = (long) Math.ceil((double) this.WS / (double) this.WA);
//             this.WS_WA_ceil_minus_1 = this.WS_WA_ceil - 1L;
//         }
//     }

//     public long getContributingWindows(long ts) {
//         return ts % this.WA >= this.WS % this.WA && this.WS % this.WA != 0L ? this.WS_WA_ceil_minus_1 : this.WS_WA_ceil;
//     }

//     public long getEarliestWinStartTS(long ts) {
//         return (long) Math.max((double) ((ts / this.WA - this.getContributingWindows(ts) + 1L) * this.WA), 0.0);
//     }

//     // public boolean canRun() {
//     //     return super.canRun();
//     // }

//     protected void checkIncreasingTimestamps(IN t) {
//         if (this.firstTuple) {
//             this.firstTuple = false;
//         } else if (getTimestamp.apply(t) < this.latestTimestamp) {
//             throw new RuntimeException("Input tuple's timestamp decreased!");
//         }

//     }

//     private Function<Queue<IN>, Queue<OUT>> aggF;
//     private Function<IN, K> getKey;
//     private Function<IN, Long> getTimestamp;
//     private TreeMap<Long, HashMap<K, Queue<IN>>> windows;
//     private long earliestWinLeftBoundary = -1;

//     @Override
//     public void enable() {
//         super.enable();
//     }

//     @Override
//     public void disable() {
//         super.disable();
//     }

//     public List<OUT> processTupleIn1(IN t) {

//         List<OUT> result = new LinkedList<OUT>();

//         checkIncreasingTimestamps(t);

//         long latestTimestamp = getTimestamp.apply(t);

//         long tL = getEarliestWinStartTS(latestTimestamp);

//         while (earliestWinLeftBoundary != -1 && earliestWinLeftBoundary < tL && !windows.isEmpty()) {

//             if (windows.firstKey() < tL) {
//                 earliestWinLeftBoundary = windows.firstKey();
//                 for (Entry<K, Queue<IN>> entry : windows.firstEntry().getValue().entrySet()) {
//                     result.addAll(aggF.apply(entry.getValue()));
//                     while (!entry.getValue().isEmpty()
//                             && getTimestamp.apply(entry.getValue().peek()) < earliestWinLeftBoundary + WA) {
//                         entry.getValue().poll();
//                     }
//                     if (!entry.getValue().isEmpty()) {
//                         if (!windows.containsKey(earliestWinLeftBoundary + WA)) {
//                             windows.put(earliestWinLeftBoundary + WA, new HashMap<>());
//                         }
//                         windows.get(earliestWinLeftBoundary + WA).put(entry.getKey(), entry.getValue());
//                     }
//                 }
//                 windows.pollFirstEntry();
//                 if (!windows.isEmpty()) {
//                     earliestWinLeftBoundary = windows.firstKey();
//                 }
//             } else {
//                 break;
//             }

//         }

//         K k = getKey.apply(t);
//         if (!windows.containsKey(tL)) {
//             windows.put(tL, new HashMap<>());
//         }
//         if (!windows.get(tL).containsKey(k)) {
//             windows.get(tL).put(k, new LinkedList<>());
//         }
//         windows.get(tL).get(k).add(t);

//         earliestWinLeftBoundary = tL;

//         return result;
//     }

// }
