package com.example.jrpc.nettyrpc.netty.handler;

import com.example.jrpc.nettyrpc.netty.message.IMessage;

/**
 * Created by yilong on 2018/3/7.
 */
public abstract class INettyChannelHandler<T extends IMessage> {
    /** Handles the receipt of a single message. */
    public abstract void handle(T message) throws Exception;

    /** Invoked when the channel this INettyChannelHandler is on is active. */
    public abstract void channelActive();

    /** Invoked when an exception was caught on the Channel. */
    public abstract void exceptionCaught(Throwable cause);

    /** Invoked when the channel this INettyChannelHandler is on is inactive. */
    public abstract void channelInactive();
}
