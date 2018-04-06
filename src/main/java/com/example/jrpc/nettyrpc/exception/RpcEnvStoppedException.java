package com.example.jrpc.nettyrpc.exception;

/**
 * Created by yilong on 2018/4/4.
 */
public class RpcEnvStoppedException extends IllegalStateException {
    public RpcEnvStoppedException() {
        super("RpcEnv already stopped.");
    }
}
