package com.storm.demo.guaranteed_message.entity;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * 以sessionId 和 group做为分组的主键, 重写equals方法
 *
 * @author zyzdisciple
 * @date 2019/4/7
 */
public class GroupKey {

    private String sessionId;

    private Long timeGroup;

    public GroupKey(String sessionId, Long timeGroup) {
        this.sessionId = sessionId;
        this.timeGroup = timeGroup;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Long getTimeGroup() {
        return timeGroup;
    }

    public void setTimeGroup(Long timeGroup) {
        this.timeGroup = timeGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        GroupKey groupKey = (GroupKey) o;

        return new EqualsBuilder()
                .append(sessionId, groupKey.sessionId)
                .append(timeGroup, groupKey.timeGroup)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(sessionId)
                .append(timeGroup)
                .toHashCode();
    }
}
