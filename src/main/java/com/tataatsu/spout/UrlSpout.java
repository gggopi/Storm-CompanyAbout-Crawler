package com.tataatsu.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.File;
import java.util.*;

/*
 * Created by gggopi on 9/6/15.
 */
public class UrlSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    int count = 0;
    //String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away", "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };


    //List<String> sentences = new ArrayList<String>(Arrays.asList('http://www.aeti.com', "http://www.arcadiabio.com","http://www.1800flowers.com"));
    List<String> sentences = new ArrayList<String>();
    //@Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        _collector = collector;
        try {
            Scanner s = new Scanner(new File("list.txt"));


            while (s.hasNextLine()) {
                sentences.add(s.nextLine());
            }
            s.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

   // @Override
    public void nextTuple() {
        if(count == sentences.size()){
            return;
        }
        else {
            for(String sentence : sentences){
                //System.out.println(sentence + " qqqqqqqqqqqqqqqqqqqqqqqqq ");
                _collector.emit(new Values(sentence));
                count++;
            }
        }
        Utils.sleep(100);
    }

    @Override
    public void ack(Object id) {
    }


    @Override
    public void fail(Object id) {

    }

    //@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("crawl"));
    }

}
