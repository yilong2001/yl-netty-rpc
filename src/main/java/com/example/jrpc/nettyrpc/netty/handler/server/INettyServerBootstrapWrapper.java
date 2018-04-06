package com.example.jrpc.nettyrpc.netty.handler.server;

import com.example.jrpc.nettyrpc.rpc.RpcHandler;
import io.netty.channel.Channel;

/**
 * Created by yilong on 2018/3/6.
 */
public interface INettyServerBootstrapWrapper {
    RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler);
}
