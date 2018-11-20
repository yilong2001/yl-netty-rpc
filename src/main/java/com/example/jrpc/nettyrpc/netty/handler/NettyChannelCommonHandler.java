package com.example.jrpc.nettyrpc.netty.handler;

import com.example.jrpc.nettyrpc.rpc.HostPort;
import com.example.jrpc.nettyrpc.rpc.RpcConfig;
import com.example.jrpc.nettyrpc.netty.handler.client.ConcurrentNettyChannelClient;
import com.example.jrpc.nettyrpc.netty.handler.client.NettyChannelResponseHandler;
import com.example.jrpc.nettyrpc.netty.handler.server.NettyChannelRequestHandler;
import com.example.jrpc.nettyrpc.netty.message.req.RequestMessage;
import com.example.jrpc.nettyrpc.netty.message.rsp.ResponseMessage;
import com.example.jrpc.nettyrpc.rpc.RpcHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;

/**
 * Created by yilong on 2018/3/7.
 * common handler for server and client at same time
 */
public class NettyChannelCommonHandler extends ChannelInboundHandlerAdapter {
    private static final Log logger = LogFactory.getLog(NettyChannelCommonHandler.class);

    private final NettyChannelRequestHandler requestHandler;
    private final NettyChannelResponseHandler responseHandler;
    private final ConcurrentNettyChannelClient client;

    private final RpcHandler rpcHandler;

    private final int requestTimeoutNs;
    private final boolean closeIdleConnections;

    public NettyChannelCommonHandler(Channel channel, RpcConfig config, RpcHandler rpcHandler) {
        //TODO:
        HostPort remoteHostPort = getRemoteHostPort(channel);

        this.rpcHandler = rpcHandler;
        this.requestHandler = new NettyChannelRequestHandler(channel, remoteHostPort, rpcHandler);
        this.responseHandler = new NettyChannelResponseHandler(channel);
        this.client = new ConcurrentNettyChannelClient(channel, responseHandler);
        this.requestTimeoutNs = config.getRequestTimeoutNs();
        this.closeIdleConnections = false;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("Exception in connection from " + getRemoteAddress(ctx.channel()),
                cause);
        requestHandler.exceptionCaught(cause);
        responseHandler.exceptionCaught(cause);
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelActive();
        } catch (RuntimeException e) {
            logger.error("Exception from request handler while channel is active", e);
        }
        try {
            responseHandler.channelActive();
        } catch (RuntimeException e) {
            logger.error("Exception from response handler while channel is active", e);
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelInactive();
        } catch (RuntimeException e) {
            logger.error("Exception from request handler while channel is inactive ", e);
        }
        try {
            responseHandler.channelInactive();
        } catch (RuntimeException e) {
            logger.error("Exception from response handler while channel is inactive", e);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object request) throws Exception {
        if (request instanceof RequestMessage) {
            requestHandler.handle((RequestMessage) request);
        } else if (request instanceof ResponseMessage) {
            responseHandler.handle((ResponseMessage) request);
        } else {
            ctx.fireChannelRead(request);
        }
    }

    /** Triggered based on events from an {@link io.netty.handler.timeout.IdleStateHandler}. */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            // See class comment for timeout semantics. In addition to ensuring we only timeout while
            // there are outstanding requests, we also do a secondary consistency check to ensure
            // there's no race between the idle timeout and incrementing the numOutstandingRequests
            // (see SPARK-7003).
            //
            // To avoid a race between TransportClientFactory.createClient() and this code which could
            // result in an inactive client being returned, this needs to run in a synchronized block.
            synchronized (this) {
                boolean isActuallyOverdue =
                        System.nanoTime() - responseHandler.getTimeOfLastRequestNs() > requestTimeoutNs;
                if (e.state() == IdleState.ALL_IDLE && isActuallyOverdue) {
                    if (responseHandler.numOutstandingRequests() > 0) {
                        String address = getRemoteAddress(ctx.channel());
                        logger.error("Connection to "+address+" has been quiet for "+requestTimeoutNs / 1000 / 1000+" ms while there are outstanding " +
                                "requests. Assuming connection is dead; please adjust spark.network.timeout if " +
                                "this is wrong.");
                        client.timeOut();
                        ctx.close();
                    } else if (closeIdleConnections) {
                        // While CloseIdleConnections is enable, we also close idle connection
                        client.timeOut();
                        ctx.close();
                    }
                }
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    public NettyChannelResponseHandler getResponseHandler() { return responseHandler; }
    public NettyChannelRequestHandler getRequestHandler() { return requestHandler; }
    public ConcurrentNettyChannelClient getClient() { return client; }

    public static String getRemoteAddress(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            return channel.remoteAddress().toString();
        }
        return "<unknown remote>";
    }

    public static HostPort getRemoteHostPort(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            if (channel.remoteAddress() instanceof InetSocketAddress) {
                InetSocketAddress addr = (InetSocketAddress)(channel.remoteAddress());
                return new HostPort (addr.getHostString(), addr.getPort());
            }
            return null;
        }
        return null;
    }
}
