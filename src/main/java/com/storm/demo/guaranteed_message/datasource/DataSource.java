package com.storm.demo.guaranteed_message.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 自己设计的数据源， 需要完成一系列功能。
 * @author zyzdisciple
 * @date 2019/4/11
 */
public enum DataSource {

    INSTANCE;

    private BufferedReader br;

    private BlockingQueue<Node> queue;

    private AtomicLong seq;

    private BlockingDeque<Long> ackIndexes;

    private static final Object deleteQueueLock = new Object();

    private static final Logger logger = LoggerFactory.getLogger(DataSource.class);

    DataSource() {
        try {
            br = new BufferedReader(new FileReader("E:\\IdeaProjects\\storm_demo\\src\\main\\resources\\user_behavior_data.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        queue = new LinkedBlockingDeque<>();
        ackIndexes = new LinkedBlockingDeque<>();
    }

    /**
     * 获取一行数据，没有返回null。
     * @return
     */
    public Node nextLine() {
        Node node = null;
        try {
            String line = br.readLine();
            if (!line.trim().isEmpty()) {
                node = new Node(seq.getAndIncrement(), line);
                queue.add(node);
            }
        } catch (IOException e) {
            logger.warn("empty queue, e:" + e);
        }
        return node;
    }

    /**
     * 成功响应
     * @param seq
     */
    public void ack(Object seq) {
        if (seq == null) {
            return;
        }
        deleteNode(Long.parseLong(seq.toString()));
    }

    private void deleteNode(long seq) {
        synchronized (deleteQueueLock) {
            Node headNode = queue.peek();
            if (headNode != null && headNode.getIndex() == seq) {
                queue.poll();
                //一直向下删除， 直到不等
                deepDelete();
            } else {
                Long headIndex = ackIndexes.peek();
                if (headIndex == null) {
                    ackIndexes.add(seq);
                } else if (seq > headIndex) {
                    ackIndexes.addLast(seq);
                } else if (seq < headIndex) {
                    ackIndexes.addFirst(seq);
                }
            }
        }
    }

    /**
     * 继续向下删除
     */
    private void deepDelete() {
        Node headNode = queue.peek();
        long seq = ackIndexes.peek();
        boolean hasDeleted = false;
        if (headNode != null && headNode.getIndex() == seq) {
            queue.poll();
            ackIndexes.poll();
            //一直向下删除， 直到不等
            deepDelete();
        }
    }
}
