package org.javameetup.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 7/14/13
 * Time: 10:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class PrinterBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
         this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("Results:"+ tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
