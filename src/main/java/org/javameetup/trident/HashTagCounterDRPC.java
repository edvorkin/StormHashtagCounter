package org.javameetup.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 7/12/13
 * Time: 7:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class HashTagCounterDRPC {



        public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
            FixedBatchSpout spout = new FixedBatchSpout(new Fields("twits"), 20,
                    new Values("Acne Guidelines Endorsed by American Academy of #Pediatrics"),
                    new Values("New Exercise-Induced #Bronchoconstriction Guidelines"),
                    new Values("Can #Colonoscopy Remain Cost-effective for Colorectal #Cancer Screening?"),
                    new Values("Non-Small Cell Lung #Cancer"),
                    new Values("Five More #Physicians Indicted in Massive Fraud Case"),
                    new Values("#Physicians and #Pharmacists Indicted in Giant Pain Drug Scam"),
                    new Values("#Aortic #Dissection Mortality Linked to Low Surgical Volume"),
                    new Values("The Treatment of Type B #Aortic #Dissection: Expert Advice"),
                    new Values("Critical Care #Physician Compensation Report: 2013"),
                    new Values("An #Oncology Consult -- With a Computer? #Cancer"),
                    new Values("#Menopause and Marijuana Will Be Featured at AACE 2013"),
                    new Values("Internet-Based Programs Help Youths Tackle Type 1 #Diabetes"),
                    new Values("#Physician Lifestyles -- Linking to Burnout: A Medscape Survey"),
                    new Values("Frustrated by Patients With #Hypochondria? What to Do"));

            spout.setCycle(true);

            // In this state we will save the real-time counts per date for each hashtag
            StateFactory mapState = new MemoryMapState.Factory();

            TridentTopology topology = new TridentTopology();

            TridentState hashcount=topology.newStream("spout", spout)
                    .each(new Fields("twits"), new Split(), new Fields("word"))
                    .each(new Fields("word"), new HashTagFilter(), new Fields("hashtag"))
                    .groupBy(new Fields("hashtag"))
                    .persistentAggregate(mapState, new Fields("hashtag"), new Count(),new Fields("count"))
                    .parallelismHint(16);


            // DRPC service:
            // Accepts a "hashtag" argument
   topology.newDRPCStream("hashtags", drpc)
           .each(new Fields("args"), new Split(), new Fields("word"))
           .each(new Fields("word"), new HashTagFilter(), new Fields("hashtag"))
           .groupBy(new Fields("hashtag"))
           .stateQuery(hashcount, new Fields("hashtag"), new MapGet(), new Fields("count"))
           .each(new Fields("word", "count"), new FilterNull());


            return topology.build();
        }

        public static void main(String[] args) throws Exception {
            Config conf = new Config();
            conf.setMaxSpoutPending(20);
            //conf.setDebug(true);
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("meetup", conf, buildTopology(drpc));
            for(int i=0; i<100; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("hashtags", "#Cancer #Physicians"));
                Thread.sleep(1000);
            }


        }





    }


