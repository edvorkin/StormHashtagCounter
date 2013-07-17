package org.javameetup.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.javameetup.bolt.HashTagCount;
import org.javameetup.bolt.HashTagFilterBolt;
import org.javameetup.bolt.PrinterBolt;
import org.javameetup.bolt.SplitSentenceBolt;
import org.javameetup.spout.RandomTweetSpout;
import storm.trident.testing.FixedBatchSpout;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 7/14/13
 * Time: 10:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class HashTagCountTopology {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomTweetSpout(),2);
        builder.setBolt("split", new SplitSentenceBolt(), 4)
               .shuffleGrouping("spout");
        builder.setBolt("hashtagFilter",new HashTagFilterBolt(),6).
                shuffleGrouping("split");
        builder.setBolt("count",new HashTagCount(),8).
                fieldsGrouping("hashtagFilter", new Fields("hashtag"));
        builder.setBolt("printer",new PrinterBolt()).
                shuffleGrouping("count");
        Config conf = new Config();


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("hashtag-count", conf,
                builder.createTopology());
        Thread.sleep(10000);

        cluster.shutdown();

    }
}
