package com.example.jrpc.nettyrpc.rpc;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yilong on 2018/3/6.
 */
public class RpcConfig {
    private int serverThreads = 5;
    private int clientThreads = 5;
    private String moudleName = "RPC";
    private boolean referDirectBufs = true;
    private int rpcAwaitTimeoutMs = 1000 * 100;
    private int rpcAskWaitTimeoutMs = 1000 * 100;
    private int nettyBackLog = 0;
    private int nettyReceiveBuf = 0;
    private int nettySendBuf = 0;
    private int nettyRequestTimeoutNs = 1000*60*60;
    private int nettyConnectionTimeoutMs = 1000*60;
    private int rpcMaxRetries = 1;
    private int rpcRetryWaitMs = 1000;
    private int rpcConnectTaskThreads = 20;

    private Map<String, Object> settings = new HashMap<>();

    public void set(String key, Object value) {
        settings.put(key, value);
    }

    public Object get(String key) {
        return settings.get(key);
    }

    public String getModuleName() {
        return moudleName;
    }

    public void setModuleName(String moudleName) {
        this.moudleName = moudleName;
    }

    public int getServerThreads() {
        return serverThreads;
    }

    public void setServerThreads(int threads) {
        this.serverThreads = threads;
    }

    public int getClientThreads() {
        return clientThreads;
    }

    public void setClientThreads(int threads) {
        this.clientThreads = threads;
    }

    public boolean getPreferDirectBufs() {
        return referDirectBufs;
    }

    public void setPreferDirectBufs(boolean referDirectBufs) {
        this.referDirectBufs = referDirectBufs;
    }

    public int getBackLog() {
        return nettyBackLog;
    }

    public void setBackLog(int logs) {
        this.nettyBackLog = logs;
    }

    public int getReceiveBuf() {
        return nettyReceiveBuf;
    }

    public void setReceiveBuf(int buf) {
        this.nettyReceiveBuf = buf;
    }

    public int getSendBuf() {
        return nettySendBuf;
    }

    public void setSendBuf(int buf) {
        this.nettySendBuf = buf;
    }

    public int getRequestTimeoutNs() { return nettyRequestTimeoutNs; }

    public void setRequestTimeoutNs(int timeoutNs) {
        this.nettyRequestTimeoutNs = timeoutNs;
    }

    public int getConnectionTimeoutMs() { return nettyConnectionTimeoutMs; }

    public void setConnectionTimeoutMs(int timeoutNs) {
        this.nettyConnectionTimeoutMs = timeoutNs;
    }

    public int getRpcAwaitTimeoutMs() { return rpcAwaitTimeoutMs; }

    public void setRpcAwaitTimeoutMs(int timeoutNs) {
        this.rpcAwaitTimeoutMs = timeoutNs;
    }

    public int getMaxRetries() { return rpcMaxRetries; }

    public void setMaxRetries(int retries) {
        this.rpcMaxRetries = retries;
    }

    public int getRetryWaitMs() { return rpcRetryWaitMs; }

    public void setRetryWaitMs(int waitMs) {
        this.rpcRetryWaitMs = waitMs;
    }

    public int getAskTimeoutMs() { return rpcAskWaitTimeoutMs; }

    public void setAskTimeoutMs(int timeoutMs) {
        this.rpcAskWaitTimeoutMs = timeoutMs;
    }

    public int getConnectTaskThreads() {
        return rpcConnectTaskThreads;
    }

    public void setConnectTaskThreads(int taskThreads) {
        this.rpcConnectTaskThreads = taskThreads;
    }
}
