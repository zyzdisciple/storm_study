package com.storm.demo.guaranteed_message.entity;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        MessageInfo that = (MessageInfo) o;

        return new EqualsBuilder()
                .append(sessionId, that.sessionId)
                .append(userId, that.userId)
                .append(endTime, that.endTime)
                .append(end, that.end)
                .append(content, that.content)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(sessionId)
                .append(userId)
                .append(endTime)
                .append(end)
                .append(content)
                .toHashCode();
    }
}
