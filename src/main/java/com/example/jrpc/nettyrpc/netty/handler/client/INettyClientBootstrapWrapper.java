package com.example.jrpc.nettyrpc.netty.handler.client;

import io.netty.channel.Channel;

/**
 * Created by yilong on 2018/3/14.
 */
public interface INettyClientBootstrapWrapper {
    void doBootstrap(ConcurrentNettyChannelClient client, Channel channel);
}
