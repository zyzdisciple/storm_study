package com.storm.demo.guaranteed_message.bolt;

import com.storm.demo.guaranteed_message.BehaviorConstants;
import com.storm.demo.guaranteed_message.entity.GroupKey;
import com.storm.demo.guaranteed_message.entity.MessageInfo;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author zyzdisciple
 * @date 2019/4/7
 */
public class ContentStitchingBolt extends BaseRichBolt {

    private static final long serialVersionUID = -4684689172584740403L;

    private OutputCollector collector;

    /**
     * 存储每个时间段的数据, 由于每个时间段可能有多组session数据, 因此放入list中处理.
     */
    private Map<GroupKey, List<Tuple>> collectMap;

    /**
     * 最好使用RotatingMap，或者其他具有定时功能的Map
     */
    private Map<GroupKey, MessageInfo> hasEmitMessage;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        collectMap = new HashMap<>();
        hasEmitMessage = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        if (!(input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID))) {
            //在这里, 首先获取数据, 如果本身为true, 且在分组中不存在对应的数据, 则可以直接发送.
            MessageInfo info = (MessageInfo) input.getValueByField(BehaviorConstants.FIELD_INFO);
            long timeGroup = input.getLongByField(BehaviorConstants.FIELD_TIME_GROUP);
            GroupKey key = new GroupKey(info.getSessionId(), timeGroup);
            //如果已结束, 直接发送数据即可
            if (info.getEnd()) {
                emitTuples(input, key);
            } else {
                //存储tuple
                saveNewTuple(input, key);
            }
        } else {
            final Long timeGroup = System.currentTimeMillis() / (BehaviorConstants.SESSION_TIME_OUT_SECS * 1000);
            Iterator<Map.Entry<GroupKey, List<Tuple>>> iterator = collectMap.entrySet().iterator();
            while(iterator.hasNext()) {
                Map.Entry<GroupKey, List<Tuple>> entry = iterator.next();
                if (entry.getKey().getTimeGroup() < timeGroup) {
                    emitTimeoutTuples(entry.getKey());
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BehaviorConstants.FIELD_INFO));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, BehaviorConstants.SESSION_TIME_OUT_SECS);
        return conf;
    }

    /**
     * 并没有做重复数据处理，重复数据处理，需要为数据本身加入唯一性标识才能做到，
     * 暂时在这里就忽略这一点。
     * @param input
     * @param key
     */
    private void saveNewTuple(Tuple input, GroupKey key) {
        if (!collectMap.containsKey(key)) {
            List<Tuple> list;
            if ((list = collectMap.get(key)).isEmpty()) {
                list = new ArrayList<>();
                //如果是第一条数据并不ack处理
            } else {
                list.add(input);
                //不是第一条数据，必须ack
                collector.ack(input);
            }
            collectMap.put(key, list);
        } else {
            //如果是已经发送过的导致重发， 则直接下发就可以了，如果采用定时，记得在这里更新时间。
            collector.emit(input, new Values(hasEmitMessage.get(key)));
            collector.ack(input);
        }

    }

    private void emitTuples(Tuple input, GroupKey key) {
        List<Tuple> tuples = collectMap.get(key);
        MessageInfo info = (MessageInfo) input.getValueByField(BehaviorConstants.FIELD_INFO);
        Tuple firstTuple;
        if (tuples == null) {
            //表明是第一条数据，此时直接ack
            firstTuple = input;
        } else {
            firstTuple = tuples.get(0);
        }
        info.setContent(getContentStitchedMessage(key) + info.getContent());
        //且锚定的时候就根据第一条数据进行锚定
        collector.emit(firstTuple, new Values(info));
        collector.ack(firstTuple);
        //发送之后直接移除
        collectMap.remove(key);
        //将已经完成发送的数据存储起来，防止因为下一环节处理错误，如数据库插入失败等。需要重新发送。
        hasEmitMessage.put(key, info);
    }

    /**
     * 发送已过期数据
     * @param key
     */
    private void emitTimeoutTuples(GroupKey key) {
        String content = getContentStitchedMessage(key);
        Tuple tuple = collectMap.get(key).get(0);
        MessageInfo info = (MessageInfo) tuple.getValueByField(BehaviorConstants.FIELD_INFO);
        info.setContent(content);
        collector.emit(tuple, new Values(info));
        collector.ack(tuple);
    }

    /**
     * 根据key值返回已经处理过的message内容
     * @return
     */
    private String getContentStitchedMessage(GroupKey key) {
        List<Tuple> tuples = collectMap.get(key);
        StringBuilder sb = new StringBuilder();
        for (Tuple tuple : tuples) {
            MessageInfo tm = (MessageInfo) tuple.getValueByField(BehaviorConstants.FIELD_INFO);
            sb.append(tm.getContent()).append(",");
        }
        return sb.toString();
    }
}
