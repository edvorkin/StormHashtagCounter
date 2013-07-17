package org.javameetup.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 7/14/13
 * Time: 10:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class RandomTweetSpout extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector;
    Random random;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
    @Override
    public void open(Map map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        random=new Random();
    }
    @Override
    public void nextTuple() {
        UUID msgId=UUID.randomUUID();
        String tweet = tweets[random.nextInt(tweets.length)];
        spoutOutputCollector.emit(new Values(tweet), msgId);
    }

    @Override
    public void ack(Object msgId) {
        // message processed successfully
    }

    @Override
    public void fail(Object msgId) {
        // message failed, can we reply?
    }
    String[] tweets = new String[] {
    "Acne Guidelines Endorsed by American Academy of #Pediatrics",
    "New Exercise-Induced #Bronchoconstriction Guidelines",
    "Can #Colonoscopy Remain Cost-effective for Colorectal #Cancer Screening?",
    "Non-Small Cell Lung #Cancer",
    "Five More #Physicians Indicted in Massive Fraud Case",
    "#Physicians and #Pharmacists Indicted in Giant Pain Drug Scam",
    "#Aortic #Dissection Mortality Linked to Low Surgical Volume",
    "The Treatment of Type B #Aortic #Dissection: Expert Advice",
    "Critical Care #Physician Compensation Report: 2013",
    "An #Oncology Consult -- With a Computer? #Cancer",
    "#Menopause and Marijuana Will Be Featured at AACE 2013",
    "Internet-Based Programs Help Youths Tackle Type 1 #Diabetes",
    "#Physician Lifestyles -- Linking to Burnout: A Medscape Survey",
    "Frustrated by Patients With #Hypochondria? What to Do"};

}
