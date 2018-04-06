package com.example.jrpc.nettyrpc.rpc;

import com.example.jrpc.nettyrpc.exception.RpcException;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by yilong on 2018/3/13.
 */
public class EndpointAddress {
    private HostPort hostPort;
    private String name;
    public EndpointAddress(HostPort address, String name) {
        this.hostPort = address;
        this.name = name;
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    public void setHostPort(HostPort hostPort) {
        this.hostPort = hostPort;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String toString() { return "LSM://"+name+"@"+hostPort.getHost()+":"+hostPort.getPort(); }

    public static  EndpointAddress parse(String url) throws RpcException {
        try {
            URI uri = new java.net.URI(url);
            String host = uri.getHost();
            int port = uri.getPort();
            String name = uri.getUserInfo();
            if (host == null || port < 0 || name == null) {
                throw new RpcException("Invalid Spark URL: " + url);
            }
            return new EndpointAddress(new HostPort(host, port), name);
        } catch (URISyntaxException e) {
            throw new RpcException("Invalid Spark URL: " + url, e);
        }
    }
}
