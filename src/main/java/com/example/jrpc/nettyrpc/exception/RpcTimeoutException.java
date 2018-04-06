package com.example.jrpc.nettyrpc.exception;

/**
 * Created by yilong on 2018/3/29.
 */
public class RpcTimeoutException extends Exception {
    public RpcTimeoutException(String msg) {
        super(msg);
    }

    public RpcTimeoutException(String msg, Throwable t) {
        super(msg, t);
    }
}
