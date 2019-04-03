package com.storm.demo.rudiments.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zyzdisciple
 * @date 2019/4/3
 */
public class CountBolt extends BaseRichBolt {

    private static final long serialVersionUID = 3693291291362580453L;

    //这里存的时候取巧，用 a1 a2 表示首字母为1，长度为1，2 的单词
    private Map<String, Integer> counterMap;

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        /*为什么hashMap也要放在这里进行初始化以后再提，这里暂时忽略。
        *在storm中， bolt和 spout的初始化一般都不会放在构造器中进行，
        * 而都是放在prepare中。
        */
        counterMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getValueByField("headWord").toString().toLowerCase() + input.getIntegerByField("wordLength");
        counterMap.put(key, countFor(key) + 1);
        counterMap.forEach((k, v) -> {
            System.out.println(k + " : " + v);
        });
    }

    /**
     * 在这里因为不需要向下一个节点下发数据， 因此不需要定义。
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    /**
     * 统计当前key已经出现多少次。
     * @param key
     * @return
     */
    private int countFor(String key) {
        Integer count =  counterMap.get(key);
        return count == null ? 0 : count;
    }

    @Override
    public void cleanup() {

    }
}
