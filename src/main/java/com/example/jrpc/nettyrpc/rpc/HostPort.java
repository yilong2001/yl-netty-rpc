package com.example.jrpc.nettyrpc.rpc;

/**
 * Created by yilong on 2018/3/13.
 */
public class HostPort {
    private String host;
    private int port;
    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }

    public String hostPort() { return host+":"+port; }
    public String toString() { return hostPort(); }
}
