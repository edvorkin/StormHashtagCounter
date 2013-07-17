package org.javameetup.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 7/12/13
 * Time: 8:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class HashTagFilter extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String word = tuple.getString(0);
        if(word.startsWith("#")) {
            // only emit hashtags, and emit them without the # character
            collector.emit(new Values(word.substring(1, word.length())));
        }
    }
}
