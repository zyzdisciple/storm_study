package com.storm.demo.guaranteed_message.spout;

import com.google.gson.Gson;
import com.storm.demo.guaranteed_message.BehaviorConstants;
import com.storm.demo.guaranteed_message.datasource.DataSource;
import com.storm.demo.guaranteed_message.datasource.Node;
import com.storm.demo.guaranteed_message.entity.MessageInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zyzdisciple
 * @date 2019/4/7
 */
public class FileReaderSpout extends BaseRichSpout {

    private static final long serialVersionUID = -148963664045319832L;

    private static final Logger logger = LoggerFactory.getLogger(FileReaderSpout.class);

    private static Gson gson;

    private DataSource dataSource;

    private Map<Long, MessageInfo> cacheMap;

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        gson = new Gson();
        dataSource = DataSource.INSTANCE;
        cacheMap = new HashMap<>();
    }

    @Override
    public void nextTuple() {
        Node node = dataSource.nextLine();
        if (node == null) {
            return;
        }
        String line = node.getValue();
        MessageInfo info = gson.fromJson(line, MessageInfo.class);
        if (isValid(info, line)) {
            completingMessage(info);
            //判断当前的时间区间.
            cacheMap.put(node.getIndex(), info);
            long timeGroup = System.currentTimeMillis() / (BehaviorConstants.SESSION_TIME_OUT_SECS * 1000);
            collector.emit(new Values(timeGroup, info), node.getIndex());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BehaviorConstants.FIELD_TIME_GROUP, BehaviorConstants.FIELD_INFO));
    }

    @Override
    public void ack(Object msgId) {
        dataSource.ack(msgId);
        cacheMap.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        long timeGroup = System.currentTimeMillis() / (BehaviorConstants.SESSION_TIME_OUT_SECS * 1000);
        collector.emit(new Values(timeGroup, cacheMap.get(msgId)), msgId);
    }

    /**
     * 补全基本数据, 设置默认值
     * @param info
     */
    private void completingMessage(MessageInfo info) {
        try {
            info.setContent(formatMessage(info.getContent()));
        } catch (Exception e) {
            logger.warn("内容转换失败:" + info);
        }
        if (info.getEndTime() == null) {
            info.setEndTime(System.currentTimeMillis());
        }

    }

    /**
     * 格式化内容，将content转为3位
     * @param content
     * @return
     */
    private String formatMessage(String content) {

        if (NumberUtils.isCreatable(content)) {
            StringBuilder sb = new StringBuilder(BehaviorConstants.CONTENT_CAPACITY);
            //从第一位不是0的数值开始
            boolean skipZero = false;
            int currentValue;
            try {
                for (String c : content.trim().split("")) {
                    //已经跳过数值为0的字符, 如果不为0将 skipZero设为true
                    //如果是+ - x等字符, 会抛出异常
                    currentValue = Integer.valueOf(c);
                    if (skipZero || (currentValue != 0 && (skipZero = true))) {
                        sb.append(currentValue);
                    }
                }
            } catch (Exception e) {
                throw new NumberFormatException();
            }
            //补全字符串
            StringBuilder result = new StringBuilder(BehaviorConstants.CONTENT_CAPACITY);
            for (int i = 0, L = sb.length(); i < BehaviorConstants.CONTENT_CAPACITY - L; i++) {
                result.append(0);
            }
            result.append(sb);
            return result.toString();
        } else {
            return BehaviorConstants.UNKNOWN_CONTENT;
        }
    }

    /**
     * message校验
     * @return
     */
    private boolean isValid(MessageInfo info, String source) {
        if (info == null) {
            logger.warn("数据格式转换失败, 字符串为:" + source);
        } else if (StringUtils.isBlank(info.getSessionId())) {
            logger.warn("缺失sessionId, 字符串为:" + source);
        } else if (StringUtils.isBlank(info.getUserId())) {
            logger.warn("缺失userId, 字符串为:" + source);
        } else {
            return true;
        }
        return false;
    }
}
