package cn.wx.demo.sls;

import cn.wx.demo.common.R;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.PutLogsResponse;
import lombok.Data;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
@RequestMapping("/api/sls")
public class LogPollController {

    private LogPollTaskManage logPollTaskManage = new LogPollTaskManage();


    private Client client = new Client("cn-hangzhou.log.aliyuncs.com", "LTAI5tBWwV7fK9X5tqUKuXen","rmaEGkpK1wumSGk4WT2qwhrmIqABwM");

    @PostMapping("/refresh")
    public R<Void> start(@RequestBody ChangePost post) {
        logPollTaskManage.refreshTasks(post.tasks);
        return R.ok(null);
    }

    @PostMapping("/put_logs")
    public R<Void> putLogs(@RequestParam(defaultValue = "1") Long num) throws LogException {
        List<LogItem> collect = Stream.generate(() -> {
            ArrayList<LogContent> ct = new ArrayList<>();
            for (int i = 0; i < 1 + ThreadLocalRandom.current().nextInt(5); i++) {
                ct.add(new LogContent(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
            }
            return new LogItem((int) (System.currentTimeMillis() / 1000), ct);
        }).limit(num).collect(Collectors.toList());
        client.PutLogs("wangxin-test", "test", "spring", collect, "server");
        return R.ok(null);
    }

    @Data
    public static class ChangePost {
        private List<LogPollConfig> tasks;
    }
}
