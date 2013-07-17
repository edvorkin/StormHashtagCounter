package org.javameetup.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 7/14/13
 * Time: 10:00 AM
 * To change this template use File | Settings | File Templates.
 */
public class SplitSentenceBolt extends BaseRichBolt {
    OutputCollector outputCollector;

    @Override
    public void prepare(Map conf, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        for (String word : sentence.split(" ")) {
            outputCollector.emit( tuple, new Values(word));
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
