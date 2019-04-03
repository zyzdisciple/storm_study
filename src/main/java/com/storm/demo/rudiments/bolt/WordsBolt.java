package com.storm.demo.rudiments.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author zyzdisciple
 * @date 2019/4/3
 */
public class WordsBolt extends BaseRichBolt {

    private static final long serialVersionUID = 520139031105355867L;

    private OutputCollector collector;

    /**
     * 与spout中的open方法功能基本一致。
     * @param stormConf
     * @param context
     * @param collector
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 类比于Spout中的nextTuple
     * @param input 接收的数据,存有数据以及其相关信息。
     */
    public void execute(Tuple input) {

        String line = input.getStringByField("line").trim();
        //input.getStringByField(DemoConstants.FIELD_LINE);
        if (!line.isEmpty()) {
            String[] words = line.split(" ");
            for (String word : words) {
                if (!word.trim().isEmpty()) {
                    collector.emit(new Values(word.charAt(0), word.length()));
                }
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("headWord", "wordLength"));
        //declarer.declare(new Fields(DemoConstants.FIELD_HEAD_WORD, DemoConstants.FIELD_WORD_LENGTH, DemoConstants.FIELD_WORD));
    }
}
