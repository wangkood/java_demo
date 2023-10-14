package cn.wx.demo.sls;


import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.PullLogsResponse;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

@Slf4j
public class LogPollTaskShard implements Runnable {

    private final LogPollSlsAdapter client;
    private final LogPollConfig config;
    private final int shardId;
    private final Consumer<FastLog> processor;
    private boolean shutdown = false;
    private final CompletableFuture<Void> shutdownFuture = new CompletableFuture();
    private RateLimiter rateLimiter;
    private String cursor;

    public LogPollTaskShard(LogPollSlsAdapter client, LogPollConfig config, int shardId,
                            Consumer<FastLog> processor) throws LogException {
        this.client = client;
        this.config = config;
        this.shardId = shardId;
        this.processor = processor;
        String cursor = client.GetCheckPoint(shardId);
        if (StringUtils.isBlank(cursor)) {
            cursor = client.GetCursor(shardId, Consts.CursorMode.BEGIN);
        }
        this.cursor = cursor;
        this.refreshRate(config.getRate());
    }

    @Override
    public void run() {
        if (shutdownFuture.isDone()) {
            return;
        }

        int unSave = 0;
        while (!shutdown) {
            PullLogsResponse logs;
            try {
                logs = fetchNextLogs();
                this.cursor = logs.getNextCursor();
            } catch (LogException e) {
                e.printStackTrace();
                LogPollUtils.sleepIgnoreInterrupt(1000);
                continue;
            }

            List<LogGroupData> logGroups;
            try {
                logGroups = logs.getLogGroups();
            } catch (LogException e) {
                e.printStackTrace();
                continue;
            }

            for (LogGroupData logGroup : logGroups) {
                log.info("{} {} {} process {}", config.theTaskKey(), config.getConsumer(), shardId, logGroup.GetFastLogGroup().getLogsCount());
                for (FastLog flg : logGroup.GetFastLogGroup().getLogs()) {
                    if (rateLimiter != null) {
                        rateLimiter.acquire();
                    }
                    unSave++;
                    log.info("{} {} {} process {}", config.theTaskKey(), config.getConsumer(), shardId, "~~~~~");
                    processor.accept(flg);
                }
            }

            if (unSave >= 30) {
                savePoint();
            }
            LogPollUtils.sleepIgnoreInterrupt(1000);
        }
        savePoint();
        shutdownFuture.complete(null);
    }

    private PullLogsResponse fetchNextLogs() throws LogException {
        int count = rateLimiter == null ? 500 : (int) Math.min(1, rateLimiter.getRate() * 10);
        return client.pullLogs(shardId, count, cursor);
    }

    private void savePoint() {
        if (cursor == null) {
            return;
        }
        try {
            client.UpdateCheckPoint(shardId, config.getConsumer(), cursor);
        } catch (LogException e) {
            log.error("save point fail", e);
        }
    }

    public void shutdown() {
        this.shutdown = true;
        try {
            LogPollUtils.waitFutureIgnoreInterrupt(shutdownFuture);
        } catch (ExecutionException e) {
            // impossible
            log.error("", e);
        }
    }

    public void refreshRate(Double shardRate) {
        if (shardRate == null) {
            this.rateLimiter = null;
        } else {
            rateLimiter = RateLimiter.create(shardRate, Duration.ofSeconds(30));
        }
    }
}
