package muSPE;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FlatMapFunctions {

    private static Function<Queue<TupleWikipediaUpdate>, Queue<TupleWikipediaUpdate>> topKOriginalEntryBasedOnLengthNative(
            int k, int minLength) {
        return new Function<Queue<TupleWikipediaUpdate>, Queue<TupleWikipediaUpdate>>() {

            @Override
            public Queue<TupleWikipediaUpdate> apply(Queue<TupleWikipediaUpdate> ts) {
                Queue<TupleWikipediaUpdate> result = new LinkedList<>();
                for (TupleWikipediaUpdate t : ts) {
                    List<String> words = findMostFrequentWordsAndReturnBasedOnLength(t.getOrig(), k, minLength);
                    for (String w : words) {
                        result.add(new TupleWikipediaUpdate(t.getTimestamp(), w, "",
                                "", t.getStimulus(), t.getUniqueID()));
                    }

                }
                return result;
            }
        };
    }

    private static Function<Queue<TupleWikipediaUpdate>, Queue<TupleWikipediaUpdate>> topKAllEntriesBasedOnLengthNative(
            int k,
            int minLength) {

        return new Function<Queue<TupleWikipediaUpdate>, Queue<TupleWikipediaUpdate>>() {

            @Override
            public Queue<TupleWikipediaUpdate> apply(Queue<TupleWikipediaUpdate> ts) {
                Queue<TupleWikipediaUpdate> result = new LinkedList<>();
                for (TupleWikipediaUpdate t : ts) {
                    List<String> f1Words = findMostFrequentWordsAndReturnBasedOnLength(t.getOrig(), k, minLength);
                    List<String> f2Words = findMostFrequentWordsAndReturnBasedOnLength(t.getChange(), k, minLength);
                    List<String> f3Words = findMostFrequentWordsAndReturnBasedOnLength(t.getUpdated(), k, minLength);

                    for (int i = 0; i < Math.min(f1Words.size(), Math.min(f2Words.size(), f3Words.size())); i++) {
                        result.add(new TupleWikipediaUpdate(t.getTimestamp(), f1Words.get(i), f2Words.get(i),
                                f3Words.get(i), t.getStimulus(), t.getUniqueID()));
                    }
                }
                return result;
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

    public static Function<Queue<TupleWikipediaUpdate>, Queue<TupleWikipediaUpdate>> instantiateFlatMapFunctionNative(
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

}
