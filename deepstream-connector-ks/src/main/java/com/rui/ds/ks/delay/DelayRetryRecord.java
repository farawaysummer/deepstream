package com.rui.ds.ks.delay;

import java.util.LinkedHashMap;
import java.util.Map;

public class DelayRetryRecord implements java.io.Serializable {
    private String eventTopic;
    private long deadline;
    private Map<String, String> values = new LinkedHashMap<>();

    public String getEventTopic() {
        return eventTopic;
    }

    public void setEventTopic(String eventTopic) {
        this.eventTopic = eventTopic;
    }

    public long getDeadline() {
        return deadline;
    }

    public void setDeadline(long deadline) {
        this.deadline = deadline;
    }

    public Map<String, String> getValues() {
        return values;
    }

    public void setValues(Map<String, String> values) {
        this.values = values;
    }
}
