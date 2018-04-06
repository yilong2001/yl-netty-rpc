package com.example.jrpc.nettyrpc.netty.handler.server;

import com.example.jrpc.nettyrpc.netty.buffer.NioManagedBuffer;
import com.example.jrpc.nettyrpc.netty.handler.INettyChannelHandler;
import com.example.jrpc.nettyrpc.netty.message.req.OneWayMessage;
import com.example.jrpc.nettyrpc.netty.message.req.RequestMessage;
import com.example.jrpc.nettyrpc.netty.message.req.RpcRequest;
import com.example.jrpc.nettyrpc.netty.message.rsp.ResponseMessage;
import com.example.jrpc.nettyrpc.netty.message.rsp.RpcFailure;
import com.example.jrpc.nettyrpc.netty.message.rsp.RpcResponse;
import com.example.jrpc.nettyrpc.rpc.HostPort;
import com.example.jrpc.nettyrpc.rpc.RpcHandler;
import com.example.jrpc.nettyrpc.rpc.RpcResponseCallback;
import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * Created by yilong on 2018/3/11.
 *
 *  ConcurrentNettyChannelClient 在与 NettyServer 建立连接以后， 会相应在 Client 有 ReponseHandler
 *  ，在 Server 有 RequestHandler。
 *
 *  Server 的 RequestHandler 在收到 Client 的 RequestMessage 之后，会经过统一的 Dispather
 *  进行处理， 并发送给相应的 Endpoint （在 Server 注册了的 Endpoint）处理
 *
 *  RpcHandler 即为统一 Dispather ，实现消息分发逻辑
 */
public class NettyChannelRequestHandler extends INettyChannelHandler<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(NettyChannelRequestHandler.class);

    private final Channel channel;
    private final HostPort defaultClient;
    private final RpcHandler rpcHandler;

    public NettyChannelRequestHandler(Channel channel,
                                      HostPort client,
                                      RpcHandler rpcHandler) {
        this.channel = channel;
        this.defaultClient = client;
        this.rpcHandler = rpcHandler;
    }

    @Override
    public void handle(RequestMessage request) throws Exception {
        if (request instanceof RpcRequest) {
            processRpcRequest((RpcRequest) request);
        } else if (request instanceof OneWayMessage) {
            processOneWayMessage((OneWayMessage) request);
        } else {
            throw new IllegalArgumentException("Unknown request type: " + request);
        }
    }

    private void processRpcRequest(final RpcRequest req) {
        try {
            rpcHandler.receive(defaultClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
                }

                @Override
                public void onFailure(Throwable e) {
                    respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
                }
            });
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
            respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        } finally {
            req.body().release();
        }
    }

    private void processOneWayMessage(OneWayMessage req) {
        try {
            rpcHandler.receive(defaultClient, req.body().nioByteBuffer());
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() for one-way message.", e);
        } finally {
            req.body().release();
        }
    }

    @Override
    public void exceptionCaught(Throwable cause) {
        rpcHandler.exceptionCaught(cause, defaultClient);
    }

    @Override
    public void channelActive() {
        rpcHandler.channelActive(defaultClient);
    }

    @Override
    public void channelInactive() {
        rpcHandler.channelInactive(defaultClient);
    }

    /**
     * Responds to a single message with some Encodable object. If a failure occurs while sending,
     * it will be logged and the channel closed.
     */
    private void respond(ResponseMessage result) {
        SocketAddress remoteAddress = channel.remoteAddress();
        channel.writeAndFlush(result).addListener(future -> {
            if (future.isSuccess()) {
                logger.trace("Sent result {} to client {}", result, remoteAddress);
            } else {
                logger.error(String.format("Error sending result %s to %s; closing connection",
                        result, remoteAddress), future.cause());
                channel.close();
            }
        });
    }
}
