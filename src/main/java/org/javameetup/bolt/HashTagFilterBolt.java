package org.javameetup.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 7/14/13
 * Time: 10:11 AM
 * To change this template use File | Settings | File Templates.
 */
public class HashTagFilterBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
       this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        if(word.startsWith("#")) {
            // only emit hashtags, and emit them without the # character
            outputCollector.emit(new Values(word.substring(1, word.length())));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }
}
