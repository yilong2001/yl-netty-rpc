package com.example.jrpc.nettyrpc.exception;

/**
 * Created by yilong on 2018/3/18.
 */
public class RpcException extends Exception {
    public RpcException(String msg) {
        super(msg);
    }

    public RpcException(String msg, Throwable t) {
        super(msg, t);
    }
}
