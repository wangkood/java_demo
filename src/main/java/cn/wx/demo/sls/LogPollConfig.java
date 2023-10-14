package cn.wx.demo.sls;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

@Data
public class LogPollConfig {

    private String endpoint;
    private String project;
    private String logStore;
    private String consumerGroup;
    private String consumer;

    private String accessKey;

    private String accessSecret;

    /**
     * 限流 条日志每秒
     */
    private Double rate;


    public LogPollKey theTaskKey() {
        if (StringUtils.isBlank(this.endpoint) ||
                StringUtils.isBlank(this.project) ||
                StringUtils.isBlank(this.logStore) ||
                StringUtils.isBlank(this.consumerGroup) ||
                StringUtils.isBlank(this.consumer)) {
            return null;
        }
        return new LogPollKey(this.endpoint, this.project, this.logStore, this.consumerGroup, this.consumer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogPollConfig that = (LogPollConfig) o;
        return Objects.equals(endpoint, that.endpoint) &&
                Objects.equals(project, that.project) &&
                Objects.equals(logStore, that.logStore) &&
                Objects.equals(consumerGroup, that.consumerGroup) &&
                Objects.equals(consumer, that.consumer) &&
                Objects.equals(accessKey, that.accessKey) &&
                Objects.equals(accessSecret, that.accessSecret) &&
                Objects.equals(rate, that.rate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, project, logStore, consumerGroup, consumer, accessKey, accessSecret, rate);
    }
}
