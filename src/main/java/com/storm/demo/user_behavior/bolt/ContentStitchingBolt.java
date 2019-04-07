package com.storm.demo.user_behavior.bolt;

import com.storm.demo.user_behavior.BehaviorConstants;
import com.storm.demo.user_behavior.entity.GroupKey;
import com.storm.demo.user_behavior.entity.MessageInfo;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Iterator;
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
    private Map<GroupKey, MessageInfo> collectMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        collectMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        if (!(input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID))) {
            //在这里, 首先获取数据, 如果本身为true, 且在分组中不存在对应的数据, 则可以直接发送.
            MessageInfo info = (MessageInfo) input.getValueByField(BehaviorConstants.FIELD_INFO);
            long timeGroup = input.getLongByField(BehaviorConstants.FIELD_TIME_GROUP);
            GroupKey key = new GroupKey(info.getSessionId(), timeGroup);
            //根据sessionId, group双重判断, 存储
            MessageInfo messageInfo = collectMap.get(key);
            //更新info的数据
            updateNewInfoWithPreMessage(info, messageInfo);
            //如果不存在且已结束, 直接发送数据即可
            if (info.getEnd()) {
                collector.emit(new Values(info));
                //发送后需要移除相关数据
                collectMap.remove(key);
            } else {
                collectMap.put(key, info);
            }
        } else {
            final Long timeGroup = System.currentTimeMillis() / (BehaviorConstants.SESSION_TIME_OUT_SECS * 1000);
            Iterator<Map.Entry<GroupKey, MessageInfo>> iterator = collectMap.entrySet().iterator();
            while(iterator.hasNext()) {
                Map.Entry<GroupKey, MessageInfo> entry = iterator.next();
                System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~");
                System.out.println(entry.getKey().getTimeGroup());
                System.out.println(timeGroup);
                if (entry.getKey().getTimeGroup() < timeGroup) {
                    collector.emit(new Values(entry.getValue()));
                    iterator.remove();
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
     * 用以前的messageContent 更改当前message
     * @param newInfo
     * @param preInfo
     */
    private void updateNewInfoWithPreMessage(MessageInfo newInfo, MessageInfo preInfo) {
        if (preInfo != null) {
            StringBuilder sb = new StringBuilder(preInfo.getContent());
            sb.append(",").append(newInfo.getContent());
            newInfo.setContent(sb.toString());
        }
    }
}
