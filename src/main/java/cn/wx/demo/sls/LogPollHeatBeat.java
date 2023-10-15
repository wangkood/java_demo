package cn.wx.demo.sls;

import com.aliyun.openservices.log.exception.LogException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LogPollHeatBeat {

    private final LogPollSlsAdapter client;
    private final int timeoutSecoend;

    private final long intervalMills;
    private final ScheduledExecutorService scheduledExecutor;
    private final HashSet<Integer> heartShards = new HashSet<>();
    private final HashSet<Integer> assignedShards = new HashSet<>();
    private long lastSuccessTime;

    public LogPollHeatBeat(LogPollSlsAdapter client, int timeoutSecoend) {
        this.client = client;
        this.timeoutSecoend = timeoutSecoend;
        this.intervalMills = (int) (timeoutSecoend / 3.0 * 1000);
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1);
    }

    public void start() {
        this.scheduledExecutor.scheduleAtFixedRate(this::heartbeat, 0, intervalMills, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        synchronized (this) {
            try {
                this.scheduledExecutor.shutdown();
                if (!this.scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    this.scheduledExecutor.shutdownNow();
                    if (!this.scheduledExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                        log.error("stop shard fail");
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            this.heartShards.clear();
            this.assignedShards.clear();
        }
    }

    private void heartbeat() {
        synchronized (this) {
            long nowMillis = System.currentTimeMillis();
            try {
                List<Integer> assigned = client.HeartBeat(new ArrayList<>(heartShards));
                this.heartShards.addAll(assigned);
                this.assignedShards.clear();
                this.assignedShards.addAll(assigned);
                this.lastSuccessTime = nowMillis;
            } catch (LogException e) {
                log.error("Error sending heartbeat", e);
                if (nowMillis - lastSuccessTime > (timeoutSecoend * 1000L) + intervalMills) {
                    // Should already been removed from consumer group
                    assignedShards.clear();
                    log.warn("Error sending heartbeat, need clear assigned shards, {}", assignedShards);
                }
            }
        }
    }

    public void unSub(int shard) {
        synchronized (this) {
            this.heartShards.remove(shard);
        }
    }

    public Actions actions() {
        synchronized (this) {
            Actions actions = new Actions(assignedShards.size(), heartShards.size() - assignedShards.size());
            for (Integer shard : this.heartShards) {
                if (assignedShards.contains(shard)) {
                    actions.process.add(shard);
                } else {
                    actions.shutdown.add(shard);
                }
            }
            return actions;
        }
    }

    @Getter
    @AllArgsConstructor
    public static class Actions {
        private final List<Integer> process;
        private final List<Integer> shutdown;

        public Actions(int processSize, int shutdownSize) {
            this.process = new ArrayList<>(processSize);
            this.shutdown = new ArrayList<>(shutdownSize);
        }
    }

}
