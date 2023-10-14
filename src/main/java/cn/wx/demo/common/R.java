package cn.wx.demo.common;

import lombok.Data;

@Data
public class R<T> {

    public static final String CODE_OK = "Ok";

    private String code;
    private String error;
    private T data;

    public R(String code, String error, T data) {
        this.code = code;
        this.error = error;
        this.data = data;
    }

    public static<T> R<T> ok(T data) {
        return new R<>(CODE_OK, null, data);
    }
}
