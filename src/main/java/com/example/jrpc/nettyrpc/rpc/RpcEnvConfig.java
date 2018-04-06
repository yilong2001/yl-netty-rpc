package com.example.jrpc.nettyrpc.rpc;

/**
 * Created by yilong on 2018/3/18.
 */
public class RpcEnvConfig {
    final String host;
    final int port;
    final boolean isClient;
    final RpcConfig rpcConfig;

    public RpcEnvConfig(RpcConfig rpcConfig, String host, int port, boolean isClient) {
        this.rpcConfig = rpcConfig;
        this.host = host;
        this.port = port;
        this.isClient = isClient;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isClient() {
        return isClient;
    }

    public RpcConfig getRpcConfig() {
        return rpcConfig;
    }
}
