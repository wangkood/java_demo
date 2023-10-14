package cn.wx.demo.sls;

import lombok.Getter;

import java.util.Objects;

/**
 * 这些值都相同 代表是同一个任务, 也就是说不会同时运行多个key相同的任务
 */
@Getter
public class LogPollKey {

    private final String endpoint;
    private final String project;
    private final String logStore;
    private final String consumerGroup;
    private final String consumer;

    public LogPollKey(String endpoint, String project, String logStore, String consumerGroup, String consumer) {
        this.endpoint = endpoint;
        this.project = project;
        this.logStore = logStore;
        this.consumerGroup = consumerGroup;
        this.consumer = consumer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogPollKey that = (LogPollKey) o;
        return endpoint.equals(that.endpoint) &&
                project.equals(that.project) &&
                logStore.equals(that.logStore) &&
                consumerGroup.equals(that.consumerGroup) &&
                consumer.equals(that.consumer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, project, logStore, consumerGroup, consumer);
    }


    @Override
    public String toString() {
        return "LogPollKey{" +
                "endpoint='" + endpoint + '\'' +
                ", project='" + project + '\'' +
                ", logStore='" + logStore + '\'' +
                ", consumerGroup='" + consumerGroup + '\'' +
                ", consumer='" + consumer + '\'' +
                '}';
    }
}
