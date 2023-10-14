package cn.wx.demo.sls;

import com.aliyun.openservices.log.exception.LogException;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

@Slf4j
public class LogPollTask implements Runnable {

    private final LogPollConfig config;
    private final ExecutorService shardExecutor;
    private final Function<List<Integer>, List<Integer>> shardFilter;
    private final LogPollHeatBeat heatBeat;
    private final LogPollSlsAdapter client;
    private final Map<Integer, LogPollTaskShard> running = new ConcurrentHashMap<>();


    private boolean shutdown = false;
    private final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();

    public LogPollTask(LogPollConfig config, ExecutorService shardExecutor,
                       Function<List<Integer>, List<Integer>> shardFilter) throws LogException {
        this.config = config;
        this.shardExecutor = shardExecutor;
        this.shardFilter = shardFilter;
        this.client = new LogPollSlsAdapter(config);
        int consumerGroupTimeout = 60;
        this.client.createConsumerGroupIfNotExist(consumerGroupTimeout, false);
        this.heatBeat = new LogPollHeatBeat(client, consumerGroupTimeout);
    }

    @Override
    public void run() {
        log.info("{} {} start", config.theTaskKey(), config.getConsumer());
        this.heatBeat.start();
        while (!shutdown) {
            LogPollHeatBeat.Actions actions = this.heatBeat.actions();
            for (Integer shard : actions.getProcess()) {
                LogPollTaskShard task = this.running.get(shard);
                if (task == null) {
                    try {
                        log.info("{} {} {} shard_start", config.theTaskKey(), config.getConsumer(), shard);
                        task = new LogPollTaskShard(client, config, shard, i -> {
                        });
                        shardExecutor.submit(task);
                        this.running.put(shard, task);
                    } catch (LogException e) {
                        log.error("", e);
                        this.heatBeat.unSub(shard);
                    }
                }
            }
            shutdownShard(actions.getShutdown());
            LogPollUtils.sleepIgnoreInterrupt(1000);
        }
        shutdownShard(this.running.keySet());
        this.shutdownFuture.complete(null);
    }

    private void shutdownShard(Collection<Integer> shards) {
        for (Integer shard : shards) {
            LogPollTaskShard task = running.remove(shard);
            if (task != null) {
                log.info("{} {} {} shard_shutdown", config.theTaskKey(), config.getConsumer(), shard);
                task.shutdown();
            }
            heatBeat.unSub(shard);
        }
    }

    public void shutdown() {
        log.info("{} {} shutdown", config.theTaskKey(), config.getConsumer());
        this.shutdown = true;
        try {
            LogPollUtils.waitFutureIgnoreInterrupt(this.shutdownFuture);
        } catch (ExecutionException e) {
            // impossible
            log.error("", e);
        }
        this.heatBeat.shutdown();
        this.client.shutdown();
    }


    public void refresh(LogPollConfig config) throws LogException {
        log.info("{} {} refresh", config.theTaskKey(), config.getConsumer());
        this.refreshRate(config.getRate());
    }

    private void refreshRate(Double rate) throws LogException {
        Double shardRate = null;
        if (rate != null) {
            shardRate = rate / client.ListShard().size();
        }
        for (LogPollTaskShard shardTask : this.running.values()) {
            shardTask.refreshRate(shardRate);
        }
    }

    public LogPollConfig getConfig() {
        return config;
    }
}
