package com.storm.demo.rudiments;

import com.storm.demo.rudiments.bolt.CountBolt;
import com.storm.demo.rudiments.bolt.WordsBolt;
import com.storm.demo.rudiments.spout.FileReaderSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
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
        builder.setSpout(STREAM_SPOUT, new FileReaderSpout());
        builder.setBolt(STREAM_WORD_BOLT, new WordsBolt()).shuffleGrouping(STREAM_SPOUT);
        builder.setBolt(STREAM_COUNT_BOLT, new CountBolt()).shuffleGrouping(STREAM_WORD_BOLT);
        Config config = new Config();
        config.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        StormTopology topology = builder.createTopology();
        cluster.submitTopology(TOPOLOGY_NAME, config, topology);
        Utils.sleep(TEN_SECONDS);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
