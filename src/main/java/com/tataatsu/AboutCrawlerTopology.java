package com.tataatsu;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.tataatsu.spout.UrlSpout;
import org.json.simple.JSONObject;

import java.util.Map;
import java.io.FileWriter;
import java.io.IOException;

// Created by gggopi on 9/6/15.


public class AboutCrawlerTopology {
    public static class CrawlerBolt extends ShellBolt implements IRichBolt {

        public CrawlerBolt() {

            super("python", "crawlerbolt.py");
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("crawl"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
    public static class SaveJSONBolt extends BaseBasicBolt {


        //Map<String, Integer> counts = new HashMap<String, Integer>();

        //@Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            JSONObject word = (JSONObject) tuple.getValue(0);
            System.out.println(word.get("filename") + " aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" );
            String filename=word.get("filename").toString();
            try{
                writeToFile(word,filename);
            } catch (IOException e){
                e.printStackTrace();
            }


            collector.emit(new Values(word));
        }

        private void writeToFile(JSONObject obj,String filename) throws IOException{
            FileWriter file = new FileWriter("output_data/"+filename+".json");
            try {
                file.write(obj.toJSONString());
                System.out.println("Successfully Copied JSON Object to File...");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                file.flush();
                file.close();
            }
        }

        //@Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("about"));
        }

    }
    public static void main(String args[]) throws Exception{
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), 1);

        builder.setBolt("crawl", new CrawlerBolt(), 20).shuffleGrouping("spout");
        builder.setBolt("save", new SaveJSONBolt(), 20).shuffleGrouping("crawl");

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(10);
            conf.setNumWorkers(10);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("crawler", conf, builder.createTopology());

            Thread.sleep(1000000);

            cluster.shutdown();
        }
    }
}
