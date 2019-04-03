package com.storm.demo.rudiments;

import com.storm.demo.rudiments.bolt.CountBolt;
import com.storm.demo.rudiments.bolt.WordsBolt;
import com.storm.demo.rudiments.spout.FileReaderSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * @author zyzdisciple
 * @date 2019/4/3
 */
public class WordCountTopology {

    private static final String STREAM_SPOUT = "spoutStream";

    private static final String STREAM_WORD_BOLT = "wordBoltStream";

    private static final String STREAM_COUNT_BOLT = "countBoltStream";

    private static final String TOPOLOGY_NAME = "rudimentsTopology";

    private static final Long TEN_SECONDS = 1000L * 10;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        //设置Spout，第一个参数为节点名称， 第二个为对应的Spout实例
        builder.setSpout(STREAM_SPOUT, new FileReaderSpout());
        //设置bolt，在这里采用随机分组即可，在shuffleGrouping，中第一个参数为接收的节点名称，表示从哪个节点接收数据
        //这里并不能等同于流名称，这个概念还有其他用处。
        builder.setBolt(STREAM_WORD_BOLT, new WordsBolt()).shuffleGrouping(STREAM_SPOUT);
        //在这里采取的是fieldsGrouping，原因则是因为在CountBolt中存在自有Map，必须保证属性一致的分到同一个bolt实例中
        builder.setBolt(STREAM_COUNT_BOLT, new CountBolt()).fieldsGrouping(STREAM_WORD_BOLT, new Fields("headWord", "wordLength"));
        //相关配置
        Config config = new Config();
        config.setDebug(true);
        //本地集群
        LocalCluster cluster = new LocalCluster();
        //通过builder创建拓扑
        StormTopology topology = builder.createTopology();
        //提交拓扑
        cluster.submitTopology(TOPOLOGY_NAME, config, topology);
        //停留几秒后关闭拓扑，否则会永久运行下去
        Utils.sleep(TEN_SECONDS);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
