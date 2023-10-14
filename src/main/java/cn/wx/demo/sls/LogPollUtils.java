package cn.wx.demo.sls;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
public class LogPollUtils {

    public static void sleepIgnoreInterrupt(long mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            log.error("log poll sleep interrupted", e);
            Thread.currentThread().interrupt();
        }
    }


    public static <T> T waitFutureIgnoreInterrupt(Future<T> future) throws ExecutionException {
        while (true) {
            try {
                return future.get();
            } catch (InterruptedException e) {
                log.error("log poll wait future interrupted, continue wait", e);
            }
        }
    }
}
