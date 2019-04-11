package com.storm.demo.guaranteed_message.bolt;

import com.google.gson.Gson;
import com.storm.demo.guaranteed_message.BehaviorConstants;
import com.storm.demo.guaranteed_message.entity.MessageInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

/**
 * @author zyzdisciple
 * @date 2019/4/7
 */
public class MessageWriterBolt extends BaseRichBolt {

    private static final long serialVersionUID = 5411259920685235771L;

    private static final Logger logger = LoggerFactory.getLogger(MessageWriterBolt.class);

    private PrintWriter pw;

    private static Gson gson;

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            pw = new PrintWriter(new FileWriter("E:\\IdeaProjects\\storm_demo\\src\\main\\resources\\user_behavior_data_write.txt"));
            gson = new Gson();
            this.collector = collector;
        } catch (IOException e) {
            logger.error("文件有误,保存失败");
        }

    }

    @Override
    public void execute(Tuple input) {
        MessageInfo info = (MessageInfo) input.getValueByField(BehaviorConstants.FIELD_INFO);
        String jsonMessage = null;
        try {
            jsonMessage = gson.toJson(info, MessageInfo.class);
        } catch (Exception e) {
            logger.warn("格式转换失败, e" + e);
            collector.ack(input);
        }
        try {
            pw.println(jsonMessage);
            pw.flush();
        } catch (Exception e) {
            logger.error("写入文件失败， e:" + e);
            collector.fail(input);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        if (pw != null) {
            pw.close();
        }
    }
}
