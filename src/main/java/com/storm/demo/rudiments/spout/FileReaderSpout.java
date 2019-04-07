package com.storm.demo.rudiments.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * @author zyzdisciple
 * @date 2019/4/3
 */
public class FileReaderSpout extends BaseRichSpout {

    private static final long serialVersionUID = -1379474443608375554L;

    private SpoutOutputCollector collector;

    private BufferedReader br;

    /**
     * 方法是用来初始化一些资源类，具体的参数需要待对storm有了更深入的了解之后再度来看。
     * 这些资源类不仅仅是参数提供的资源， 包括读取文件， 读取数据库，等等其他任何方式，
     * 打开数据资源都是在这个方法中实现。
     * 原因则是可以理解为，当对象被初始化时执行的方法，并不准确，但可以这样理解。
     * @param conf
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        try {
            br = new BufferedReader(new FileReader("E:\\IdeaProjects\\storm_demo\\src\\main\\resources\\data.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 流的核心，不断调用这个方法，读取数据，发送数据。
     * 在这里采取的方式是每次读取一行，当然也可以在一次中读取所有数据，然后在循环中
     * emit发射数据。
     * 需要特别注意的是，这个方法一定是不能够被阻塞的， 也不能够抛出异常，
     * 抛出异常会让当然程序停止，阻塞严重影响性能。
     */
    @Override
    public void nextTuple() {
        try {
            //向外发射数据
            String line = br.readLine();
            if (line == null) {
                return;
            }
            collector.emit(new Values(line));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 定义输出格式，在collector.emit时，new values可接受数组， 如发送 a b c，
     * 则此时会与declare field中的名称一一对应，且顺序一致，并且必须保证数量一致。
     * 通过这种配置的方式，就无需以map形式输出数据， 我们可以仅输出值即可。
     *
     * 当然declare不止这一种重载方法，其余的暂时不用理会。
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /*Fields名称这里，一般使用中会拆出来，定义为常量，而不是直接字符串，
        * 包括Stream等其他属性也是，因为很有可能在其他地方会被用到，所以一般拆分成常量
        * */
        declarer.declare(new Fields("line"));
        //declarer.declare(new Fields(DemoConstants.FIELD_LINE)); //应该采取这种方式
    }

    /**
     * 在fileReader结束之后关闭对应的流
     */
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
}
