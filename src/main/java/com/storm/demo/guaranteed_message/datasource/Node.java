package com.storm.demo.guaranteed_message.datasource;

/**
 * @author zyzdisciple
 * @date 2019/4/11
 */
public class Node {
    private long index;
    private String value;
    Node(long index, String value) {
        this.index = index;
        this.value = value;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
