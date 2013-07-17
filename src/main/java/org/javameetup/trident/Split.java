package org.javameetup.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 7/12/13
 * Time: 6:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class Split extends BaseFunction {


    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

            String sentence = tuple.getString(0);
            for(String word: sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }


