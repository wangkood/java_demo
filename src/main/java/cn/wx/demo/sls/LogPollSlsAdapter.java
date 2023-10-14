package cn.wx.demo.sls;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.http.client.ClientConfiguration;
import com.aliyun.openservices.log.request.PullLogsRequest;
import com.aliyun.openservices.log.response.ConsumerGroupCheckPointResponse;
import com.aliyun.openservices.log.response.ListConsumerGroupResponse;
import com.aliyun.openservices.log.response.PullLogsResponse;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class LogPollSlsAdapter {

    private final Client client;
    private final LogPollConfig config;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LogPollSlsAdapter(LogPollConfig config) {
        this.config = config;
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setMaxConnections(1000);
        clientConfig.setConnectionTimeout(5000);
        clientConfig.setSocketTimeout(60000);
        clientConfig.setUseReaper(true);
        this.client = new Client(config.getEndpoint(), config.getAccessKey(), config.getAccessSecret(), clientConfig);
        this.client.setUserAgent("Consumer-Library-" + config.getConsumerGroup() + "/" + config.getConsumer());
        this.client.setUseDirectMode(false);
    }

    public void shutdown() {
        freeClient();
    }

    private void freeClient() {
        if (client != null) {
            // Free resource in HTTP service client.
            client.shutdown();
        }
    }

//    public void SwitchClient(String endpoint, String accessKeyId, String accessKey, String stsToken) {
//        lock.writeLock().lock();
//        freeClient();
//        this.client = createClient(endpoint, accessKeyId, accessKey, stsToken);
//        lock.writeLock().unlock();
//    }

    private ConsumerGroup getConsumerGroup(String consumerGroupName) throws LogException {
        ListConsumerGroupResponse response = client.ListConsumerGroup(config.getProject(), config.getLogStore());
        if (response != null) {
            for (ConsumerGroup item : response.GetConsumerGroups()) {
                if (item.getConsumerGroupName().equalsIgnoreCase(consumerGroupName)) {
                    return item;
                }
            }
        }
        return null;
    }


    List<Shard> ListShard() throws LogException {
        return client.ListShard(config.getProject(), config.getLogStore()).GetShards();
    }

    void createConsumerGroupIfNotExist(int timeout, boolean order) throws LogException {
        lock.readLock().lock();
        try {
            boolean exist = false;
            try {
                ConsumerGroup consumerGroup = getConsumerGroup(this.config.getConsumerGroup());
                if (consumerGroup != null) {
                    if (consumerGroup.getTimeout() == timeout
                            && consumerGroup.isInOrder() == order) {
                        return;
                    }
                    exist = true;
                }
            } catch (LogException ex) {
                log.warn("Error checking consumer group", ex);
                // do not throw exception here for bwc
            }
            if (!exist) {
                try {
                    client.CreateConsumerGroup(config.getProject(), config.getLogStore(),
                            new ConsumerGroup(config.getConsumerGroup(), timeout, order));
                    log.info("Create ConsumerGroup {} success.", config.getConsumerGroup());
                    return;
                } catch (LogException e) {
                    if (!"ConsumerGroupAlreadyExist".equalsIgnoreCase(e.GetErrorCode())) {
                        throw e;
                    }
                }
            }
            try {
                UpdateConsumerGroup(timeout, order);
                log.info("Update ConsumerGroup {} success.", config.getConsumerGroup());
            } catch (LogException e) {
                throw e;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void CreateConsumerGroup(final int timeoutInSec, final boolean inOrder) throws LogException {
        lock.readLock().lock();
        try {
            client.CreateConsumerGroup(config.getProject(), config.getLogStore(), new ConsumerGroup(config.getConsumerGroup(), timeoutInSec, inOrder));
        } finally {
            lock.readLock().unlock();
        }
    }

    public void UpdateConsumerGroup(final int timeoutInSec, final boolean inOrder) throws LogException {
        lock.readLock().lock();
        try {
            client.UpdateConsumerGroup(config.getProject(), config.getLogStore(), config.getConsumerGroup(), inOrder, timeoutInSec);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<Integer> HeartBeat(ArrayList<Integer> shards) throws LogException {
        lock.readLock().lock();
        try {
            return client.HeartBeat(config.getProject(), config.getLogStore(), config.getConsumerGroup(), config.getConsumer(), shards).getShards();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void UpdateCheckPoint(final int shard, final String consumer, final String checkpoint) throws LogException {
        lock.readLock().lock();
        try {
            client.UpdateCheckPoint(config.getProject(), config.getLogStore(), config.getConsumerGroup(), consumer, shard, checkpoint);
        } finally {
            lock.readLock().unlock();
        }
    }

    public String GetCheckPoint(final int shard) throws LogException {
        lock.readLock().lock();
        ConsumerGroupCheckPointResponse response;
        try {
            response = client.GetCheckPoint(config.getProject(), config.getLogStore(), config.getConsumerGroup(), shard);
        } finally {
            lock.readLock().unlock();
        }
        // TODO move this to SDK
        List<ConsumerGroupShardCheckPoint> checkpoints = response.getCheckPoints();
        if (checkpoints == null || checkpoints.isEmpty()) {
            return "";
        }
        return checkpoints.get(0).getCheckPoint();
    }

    public String GetCursor(final int shard, Consts.CursorMode mode) throws LogException {
        lock.readLock().lock();
        try {
            return client.GetCursor(config.getProject(), config.getLogStore(), shard, mode).GetCursor();
        } finally {
            lock.readLock().unlock();
        }
    }

    public String getCursor(int shard, LogHubCursorPosition position, long startTime) throws LogException {
        if (position.equals(LogHubCursorPosition.BEGIN_CURSOR)) {
            return GetCursor(shard, Consts.CursorMode.BEGIN);
        } else if (position.equals(LogHubCursorPosition.END_CURSOR)) {
            return GetCursor(shard, Consts.CursorMode.END);
        } else {
            return GetCursor(shard, startTime);
        }
    }

    public String GetCursor(final int shard, final long time) throws LogException {
        lock.readLock().lock();
        try {
            return client.GetCursor(config.getProject(), config.getLogStore(), shard, time).GetCursor();
        } finally {
            lock.readLock().unlock();
        }
    }

    public PullLogsResponse pullLogs(final int shard, final int lines, final String cursor) throws LogException {
        lock.readLock().lock();
        try {
            return client.pullLogs(new PullLogsRequest(config.getProject(), config.getLogStore(), shard, lines, cursor));
        } finally {
            lock.readLock().unlock();
        }
    }
}
