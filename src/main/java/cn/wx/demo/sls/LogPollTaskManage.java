package cn.wx.demo.sls;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class LogPollTaskManage {
    private final ExecutorService taskExecutor = new ThreadPoolExecutor(5, Integer.MAX_VALUE,
            0, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
    private final ExecutorService shardExecutor = new ThreadPoolExecutor(5, Integer.MAX_VALUE,
            0, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
    private Map<LogPollKey, LogPollTask> running = Collections.emptyMap();


    public void refreshMaxShardNum(int maxShardNum) {

    }

    public void refreshTasks(List<LogPollConfig> configs) {
        synchronized (this) {
            Diff diff = findDiff(this.running, configs);
            this.running = execDiff(diff, running, taskExecutor, shardExecutor, this::shardLimit);
        }
    }



    private static Diff findDiff(Map<LogPollKey, LogPollTask> running, List<LogPollConfig> configsRaw) {
        Diff diff = new Diff();
        Map<LogPollKey, LogPollConfig> configs = configsRaw.stream()
                .collect(HashMap::new, (m, v) -> {
                    LogPollKey key = v.theTaskKey();
                    if (key != null) {
                        m.put(v.theTaskKey(), v);
                    }
                }, HashMap::putAll);
        // running 1 2 3
        // configs   2 3 4
        // start         4
        // refresh   2 3
        configs.forEach((key, config) -> {
            LogPollTask task = running.get(key);
            if (task != null) {
                if (!config.equals(task.getConfig())) {
                    diff.refresh.put(key, new TaskAndRefreshConfig(task, config));
                }
                return;
            }
            diff.starts.put(key, config);
        });
        // running 1 2 3
        // configs   2 3 4
        // stop    1
        running.forEach((key, task) -> {
            if (!configs.containsKey(key)) {
                diff.shutdowns.put(key, task);
            }
        });
        return diff;
    }

    private static class Diff {
        private final Map<LogPollKey, LogPollConfig> starts = new HashMap<>();
        private final Map<LogPollKey, TaskAndRefreshConfig> refresh = new HashMap<>();
        private final Map<LogPollKey, LogPollTask> shutdowns = new HashMap<>();
    }

    @AllArgsConstructor
    private static class TaskAndRefreshConfig {
        private LogPollTask task;
        private LogPollConfig config;
    }

    private static Map<LogPollKey, LogPollTask> execDiff(Diff diff, Map<LogPollKey, LogPollTask> running,
                                                         ExecutorService taskExecutor, ExecutorService shardExecutor,  Function<List<Integer>, List<Integer>> shardFilter) {
        Map<LogPollKey, LogPollTask> news = new HashMap<>(running);
        diff.starts.forEach((key, config) -> {
            log.info("exec start {}", key);
            try {
                LogPollTask task = new LogPollTask(config, shardExecutor, shardFilter);
                taskExecutor.submit(task);
                news.put(key, task);
            } catch (Throwable e) {
                log.error("exec shutdown fail", e);
            }
        });
        diff.refresh.forEach((key, taskAndConfig) -> {
            log.info("exec refresh {}", key);
            try {
                taskAndConfig.task.refresh(taskAndConfig.config);
            } catch (Throwable e) {
                log.error("exec shutdown fail", e);
            }
        });
        diff.shutdowns.forEach((key, task) -> {
            log.info("exec shutdown {}", key);
            try {
                task.shutdown();
                news.remove(key);
            } catch (Throwable e) {
                news.put(key, task);
                log.error("exec shutdown fail", e);
            }
        });
        return Collections.unmodifiableMap(news);
    }

    private List<Integer> shardLimit (List<Integer> shards) {

        return shards;
    }

    public void shutdown() {
        synchronized (this) {
            Iterator<Map.Entry<LogPollKey, LogPollTask>> it = this.running.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<LogPollKey, LogPollTask> entry = it.next();
                it.remove();
                try {
                    entry.getValue().shutdown();
                } catch (Throwable e) {
                    log.error("exec shutdown fail", e);
                }
            }
        }
    }
}
