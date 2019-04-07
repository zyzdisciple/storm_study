package com.storm.demo.user_behavior.bolt;

import com.google.gson.Gson;
import com.storm.demo.user_behavior.BehaviorConstants;
import com.storm.demo.user_behavior.entity.MessageInfo;
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

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            pw = new PrintWriter(new FileWriter("E:\\IdeaProjects\\storm_demo\\src\\main\\resources\\user_behavior_data_write.txt"));
            gson = new Gson();
        } catch (IOException e) {
            logger.error("文件有误,保存失败");
        }

    }

    @Override
    public void execute(Tuple input) {
        MessageInfo info = (MessageInfo) input.getValueByField(BehaviorConstants.FIELD_INFO);
        try {
            String jsonMessage = gson.toJson(info, MessageInfo.class);
            pw.println(info);
            pw.flush();
        } catch (Exception e) {
            logger.warn("格式转换失败:" + info);
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
