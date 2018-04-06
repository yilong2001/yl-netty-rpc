package com.example.jrpc.nettyrpc.exception;

/**
 * Created by yilong on 2018/4/5.
 */
public class RpcEndpointNotFoundException extends Exception {
    public RpcEndpointNotFoundException(String msg) {
        super(msg);
    }
}
