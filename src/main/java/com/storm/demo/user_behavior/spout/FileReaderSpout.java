package com.storm.demo.user_behavior.spout;

import com.google.gson.Gson;
import com.storm.demo.user_behavior.BehaviorConstants;
import com.storm.demo.user_behavior.entity.MessageInfo;
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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * @author zyzdisciple
 * @date 2019/4/7
 */
public class FileReaderSpout extends BaseRichSpout {

    private static final long serialVersionUID = 2970891499685374549L;

    private static final Logger logger = LoggerFactory.getLogger(FileReaderSpout.class);

    private static Gson gson;

    private BufferedReader br;

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        gson = new Gson();
        try {
            br = new BufferedReader(new FileReader("E:\\IdeaProjects\\storm_demo\\src\\main\\resources\\user_behavior_data.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        try {
            String line = br.readLine();
            if (line != null && !line.isEmpty()) {
                line = line.trim();
                MessageInfo info = gson.fromJson(line, MessageInfo.class);
                //只发送有效数据,无效数据跳过
                /*在这里, 我们是逐行发送, 如果有条件的话, 也就是说一次能取出
                * 多条数据的情况下, 全部一次发送是比较好的选择.
                * */
                if (isValid(info, line)) {
                    completingMessage(info);
                    //判断当前的时间区间.
                    long timeGroup = System.currentTimeMillis() / (BehaviorConstants.SESSION_TIME_OUT_SECS * 1000);
                    collector.emit(new Values(timeGroup, info));
                }
            }
        } catch (Exception e) {
            /*在catch语句中，我们一般会选择打印log日志，表示为什么出错，并不做其他处理，
            * 即使数据格式存在问题依然需要能够正常执行下去，保证拓扑不中断。
            * 在这里可以进一步将Exception细分，对不同的Exception打印日志不同。
            * */
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BehaviorConstants.FIELD_TIME_GROUP, BehaviorConstants.FIELD_INFO));
    }

    @Override
    public void close() {
        if (br != null) {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
