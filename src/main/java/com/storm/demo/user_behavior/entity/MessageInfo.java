package com.storm.demo.user_behavior.entity;

import java.io.Serializable;

/**
 * @author zyzdisciple
 * @date 2019/4/7
 */
public class MessageInfo implements Serializable {

    private static final long serialVersionUID = -6827092391472317448L;

    private String sessionId;

    private String userId;

    private Long endTime;

    private Boolean end;

    private String content;

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public Boolean getEnd() {
        return end;
    }

    public void setEnd(Boolean end) {
        this.end = end;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "MessageInfo{" +
                "sessionId='" + sessionId + '\'' +
                ", userId='" + userId + '\'' +
                ", endTime=" + endTime +
                ", end=" + end +
                ", content='" + content + '\'' +
                '}';
    }
}
