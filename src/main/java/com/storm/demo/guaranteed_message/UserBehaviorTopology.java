package com.storm.demo.guaranteed_message;

import com.storm.demo.guaranteed_message.bolt.ContentStitchingBolt;
import com.storm.demo.guaranteed_message.bolt.MessageWriterBolt;
import com.storm.demo.guaranteed_message.spout.FileReaderSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * @author zyzdisciple
 * @date 2019/4/7
 */
public class UserBehaviorTopology {

    private static final String STREAM_SPOUT = "spout";
    private static final String STREAM_CONTENT_BOLT = "content-stitching-bolt";
    private static final String STREAM_FILE_WRITER_BOLT = "file-writer-bolt";
    private static final String TOPOLOGY_NAME = "user-behavior-topology";
    private static final long TEN_SECONDS = 1000L * 10;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(STREAM_SPOUT, new FileReaderSpout(), 4);
        //这里必然要使用field 保证同一group的数据发送到同一个bolt中.
        builder.setBolt(STREAM_CONTENT_BOLT, new ContentStitchingBolt(), 8)
                .setNumTasks(64)
                .fieldsGrouping(STREAM_SPOUT, new Fields(BehaviorConstants.FIELD_TIME_GROUP));
        builder.setBolt(STREAM_FILE_WRITER_BOLT, new MessageWriterBolt(), 4).shuffleGrouping(STREAM_CONTENT_BOLT);

        Config conf = new Config();
        //当设置为true时, 且log level定义为info时, 会在控制台输出发射元组内容
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 30000);
        StormTopology topology = builder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, topology);
        //停留几秒后关闭拓扑，否则会永久运行下去
        Utils.sleep(TEN_SECONDS);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
