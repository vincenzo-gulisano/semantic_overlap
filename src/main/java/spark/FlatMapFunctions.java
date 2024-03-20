package spark;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class FlatMapFunctions {

    private static FlatMapFunction<TupleWikipediaUpdate, TupleWikipediaUpdate> topKOriginalEntryBasedOnLengthNative(
            int k, int minLength) {
        return new FlatMapFunction<TupleWikipediaUpdate, TupleWikipediaUpdate>() {

            @Override
            public Iterator<TupleWikipediaUpdate> call(TupleWikipediaUpdate t) throws Exception {
                List<String> words = findMostFrequentWordsAndReturnBasedOnLength(t.getOrig(), k, minLength);
                List<TupleWikipediaUpdate> result = new LinkedList<>();
                for (String w : words) {
                    result.add(new TupleWikipediaUpdate(t.getTimestamp(), w, "",
                            "", t.getStimulus()));
                }
                return result.iterator();
            }
        };
    }

    private static FlatMapFunction<Row, Row> topKOriginalEntryBasedOnLengthNativeRow(
            int k, int minLength) {
        return new FlatMapFunction<Row, Row>() {

            @Override
            public Iterator<Row> call(Row t) throws Exception {
                if (t.getString(1) == null) {
                    System.out.println(t.toString());
                }
                List<String> words = findMostFrequentWordsAndReturnBasedOnLength(t.getString(1), k, minLength);
                List<Row> result = new LinkedList<>();

                for (String w : words) {
                    result.add(RowFactory.create(
                            t.getAs("timestamp"),
                            w,
                            "",
                            "",
                            t.getAs("stimulus")));
                }
                return result.iterator();
            }
        };
    }

    private static FlatMapFunction<TupleWikipediaUpdate, TupleWikipediaUpdate> topKAllEntriesBasedOnLengthNative(int k,
            int minLength) {

        return new FlatMapFunction<TupleWikipediaUpdate, TupleWikipediaUpdate>() {

            @Override
            public Iterator<TupleWikipediaUpdate> call(TupleWikipediaUpdate t) throws Exception {
                List<String> f1Words = findMostFrequentWordsAndReturnBasedOnLength(t.getOrig(), k, minLength);
                List<String> f2Words = findMostFrequentWordsAndReturnBasedOnLength(t.getChange(), k, minLength);
                List<String> f3Words = findMostFrequentWordsAndReturnBasedOnLength(t.getUpdated(), k, minLength);

                List<TupleWikipediaUpdate> result = new LinkedList<>();
                for (int i = 0; i < Math.min(f1Words.size(), Math.min(f2Words.size(), f3Words.size())); i++) {
                    result.add(new TupleWikipediaUpdate(t.getTimestamp(), f1Words.get(i), f2Words.get(i),
                            f3Words.get(i), t.getStimulus()));
                }
                return result.iterator();
            }
        };
    }

    private static FlatMapFunction<Row, Row> topKAllEntriesBasedOnLengthNativeRow(int k,
            int minLength) {

        return new FlatMapFunction<Row, Row>() {

            @Override
            public Iterator<Row> call(Row t) throws Exception {
                List<String> f1Words = findMostFrequentWordsAndReturnBasedOnLength(t.getString(1), k, minLength);
                List<String> f2Words = findMostFrequentWordsAndReturnBasedOnLength(t.getString(2), k, minLength);
                List<String> f3Words = findMostFrequentWordsAndReturnBasedOnLength(t.getString(3), k, minLength);

                List<Row> result = new LinkedList<>();
                for (int i = 0; i < Math.min(f1Words.size(), Math.min(f2Words.size(), f3Words.size())); i++) {
                    result.add(RowFactory.create(
                            t.getAs("timestamp"),
                            f1Words.get(i),
                            f2Words.get(i),
                            f3Words.get(i),
                            t.getAs("stimulus")));
                }
                return result.iterator();
            }
        };
    }

    private static List<String> findMostFrequentWordsAndReturnBasedOnLength(String s, int k, int length) {
        List<String> result = new LinkedList<>();
        Map<String, Integer> topk = findFrequentWords(s, k);
        for (String w : topk.keySet()) {
            if (w.length() >= length) {
                result.add(w);
            }
        }
        return result;
    }

    private static Map<String, Integer> findFrequentWords(String s, int numberOfWords) {
        Map<String, Integer> mapOfFrequentWords = new TreeMap<>();

        String[] words = s.split("\\s+");

        for (String word : words) {
            if (!mapOfFrequentWords.containsKey(word)) {
                mapOfFrequentWords.put(word, 1);
            } else {
                mapOfFrequentWords.put(word, mapOfFrequentWords.get(word) + 1);
            }
        }

        Map<String, Integer> sorted = mapOfFrequentWords
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .limit(numberOfWords)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                        LinkedHashMap::new));
        return sorted;
    }

    public static FlatMapFunction<TupleWikipediaUpdate, TupleWikipediaUpdate> instantiateFlatMapFunctionNative(
            ExperimentID id) {
        switch (id) {
            case LLF:
                return topKOriginalEntryBasedOnLengthNative(1, 10);
            case ALF:
                return topKOriginalEntryBasedOnLengthNative(1, 0);
            case HLF:
                return topKOriginalEntryBasedOnLengthNative(3, 0);
            case LHF:
                return topKAllEntriesBasedOnLengthNative(1, 10);
            case AHF:
                return topKAllEntriesBasedOnLengthNative(1, 0);
            case HHF:
                return topKAllEntriesBasedOnLengthNative(3, 0);
            default:
                throw new RuntimeException("Unrecognized experiment id");
        }
    }

    public static FlatMapFunction<Row, Row> instantiateFlatMapFunctionNativeRow(
            ExperimentID id) {
        switch (id) {
            case LLF:
                return topKOriginalEntryBasedOnLengthNativeRow(1, 10);
            case ALF:
                return topKOriginalEntryBasedOnLengthNativeRow(1, 0);
            case HLF:
                return topKOriginalEntryBasedOnLengthNativeRow(3, 0);
            case LHF:
                return topKAllEntriesBasedOnLengthNativeRow(1, 10);
            case AHF:
                return topKAllEntriesBasedOnLengthNativeRow(1, 0);
            case HHF:
                return topKAllEntriesBasedOnLengthNativeRow(3, 0);
            default:
                throw new RuntimeException("Unrecognized experiment id");
        }
    }
}
