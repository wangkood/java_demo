package cn.wx.demo.sls;

import lombok.Data;

@Data
public class LogPollExt {

    /**
     * 限流 条日志每秒
     */
    private Integer rate;
}
