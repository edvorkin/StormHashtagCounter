package org.javameetup.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 7/12/13
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class Skeleton {


    public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 10,
                new Values("Acne Guidelines Endorsed by American Academy of #Pediatrics"),
                new Values("New Exercise-Induced #Bronchoconstriction Guidelines"),
                new Values("Can #Colonoscopy Remain Cost-effective for Colorectal #Cancer Screening?"),
                new Values("Non-Small Cell Lung #Cancer"),
                new Values("Five More #Physicians Indicted in Massive Fraud Case"),
                new Values("#Physicians and #Pharmacists Indicted in Giant Pain Drug Scam"),
                new Values("#Aortic #Dissection Mortality Linked to Low Surgical Volume"),
                new Values("The Treatment of Type B #Aortic #Dissection: Expert Advice"),
                new Values("Critical Care #Physician Compensation Report: 2013"),
                new Values("An #Oncology Consult -- With a Computer?"),
                new Values("#Menopause and Marijuana Will Be Featured at AACE 2013"),
                new Values("Internet-Based Programs Help Youths Tackle Type 1 #Diabetes"),
                new Values("#Pediatrics Lifestyles -- Linking to Burnout: A Medscape Survey"),
                new Values("Frustrated by Patients With #Hypochondria? What to Do"));

        spout.setCycle(false);


        TridentTopology topology = new TridentTopology();

    /*    topology.newStream("spout", spout).each(new Fields("sentence"),
                new Utils.PrintFilter());*/

        /*
        topology.newStream("spout", spout).each(new Fields("sentence"), new Utils.UppercaseFunction(), new Fields("uppercased_text"))
                .each(new Fields("uppercased_text"),new Utils.PrintFilter());
         */

        topology.newStream("spout", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .partitionBy(new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(),
                        new Count(), new Fields("count")).newValuesStream()
                .each(new Fields("word", "count"), new SaveToDB(), new Fields("test"));






        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("hackaton", conf, buildTopology(drpc));
    }

    public static class SaveToDB extends BaseFunction {

        ConcurrentHashMap<String, Long> result=new ConcurrentHashMap<String, Long>(20);


        @Override
        public void cleanup() {

            for (String key : result.keySet()) {
                System.out.println(key + "  "+ result.get(key));
            }
            super.cleanup();
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String key=tuple.getString(0);
            Long value=tuple.getLong(1);
            result.putIfAbsent(key,value);

                System.out.println(key + "  "+ result.get(key));




    }
    }
}
