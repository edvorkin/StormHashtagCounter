package org.javameetup.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 7/12/13
 * Time: 3:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class Utils {

    /**
     * A filter that filters nothing but prints the tuples it sees. Useful to test and debug things.
     */
    @SuppressWarnings({ "serial", "rawtypes" })
    public static class PrintFilter implements Filter {

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
        }
        @Override
        public void cleanup() {
        }

        @Override
        public boolean isKeep(TridentTuple tuple) {
            System.out.println(tuple);
            return true;
        }
    }

    /**
     * Given a hashmap with string keys and integer counts, returns the "top" map of it. "n" specifies the size of
     * the top to return.
     */
    public final static Map<String, Integer> getTopNOfMap(Map<String, Integer> map, int n) {
        List<Map.Entry<String, Integer>> entryList = new ArrayList<Map.Entry<String, Integer>>(map.size());
        entryList.addAll(map.entrySet());
        Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {

            @Override
            public int compare(Map.Entry<String, Integer> arg0, Map.Entry<String, Integer> arg1) {
                return arg1.getValue().compareTo(arg0.getValue());
            }
        });
        Map<String, Integer> toReturn = new HashMap<String, Integer>();
        for(Map.Entry<String, Integer> entry: entryList.subList(0, Math.min(entryList.size(), n))) {
            toReturn.put(entry.getKey(), entry.getValue());
        }
        return toReturn;
    }

    /**
     * Dummy function that just emits the uppercased tweet text.
     */
    @SuppressWarnings("serial")
    public static class UppercaseFunction extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            collector.emit(new Values(tuple.getString(0).toUpperCase()));
        }
    }



}
