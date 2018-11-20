/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.jrpc.nettyrpc.netty.handler.client;

import com.example.jrpc.nettyrpc.netty.buffer.NioManagedBuffer;
import com.example.jrpc.nettyrpc.netty.message.req.OneWayMessage;
import com.example.jrpc.nettyrpc.netty.message.req.RpcRequest;
import com.example.jrpc.nettyrpc.rpc.RpcResponseCallback;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import static com.example.jrpc.nettyrpc.netty.handler.NettyChannelCommonHandler.getRemoteAddress;

/**
 * Created by yilong on 2018/3/12.
 * 支持多线程并发的 NettyChannelClient，与其关联的 Channel 能够识别不同的 Request
 * ，通过 RequestId , RequestId 在 NettyChannelResponseHandler 中存储，并等待 Rsp，
 * ，确保 Request 与 Response 一一对应，
 * NettyChannelResponseHandler 需要支持多线程 Client 的调用
 *
 * RequestMessage format on channel:
 * [is has sender address, [sender address]], [endpoint address], [Request]
 *
 * Request format on channel:
 * [Request Id], [Content]
 */
public class ConcurrentNettyChannelClient implements Closeable {
    private static final Log logger = LogFactory.getLog(ConcurrentNettyChannelClient.class);

    private final Channel channel;
    private final NettyChannelResponseHandler handler;
    @Nullable private String clientId;
    private volatile boolean timedOut;

    public ConcurrentNettyChannelClient(Channel channel, NettyChannelResponseHandler handler) {
        this.channel = Preconditions.checkNotNull(channel);
        this.handler = Preconditions.checkNotNull(handler);
        this.timedOut = false;
    }

    public Channel getChannel() {
        return channel;
    }

    public boolean isActive() {
        return !timedOut && (channel.isOpen() || channel.isActive());
    }

    public SocketAddress getSocketAddress() {
        return channel.remoteAddress();
    }

    /**
     * Returns the ID used by the client to authenticate itself when authentication is enabled.
     *
     * @return The client ID, or null if authentication is disabled.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Sets the authenticated client ID. This is meant to be used by the authentication layer.
     *
     * Trying to set a different client ID after it's been set will result in an exception.
     */
    public void setClientId(String id) {
        Preconditions.checkState(clientId == null, "Client ID has already been set.");
        this.clientId = id;
    }

    /**
     * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
     * with the server's response or upon any failure.
     *
     * @param message The message to send.
     * @param callback Callback to handle the RPC's reply.
     * @return The RPC's id.
     */
    public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
        long startTime = System.currentTimeMillis();
        if (logger.isTraceEnabled()) {
            logger.trace("Sending RPC to " + getRemoteAddress(channel));
        }

        long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
        handler.addRpcRequest(requestId, callback);

        RpcRequest rpcRequest = new RpcRequest(requestId, new NioManagedBuffer(message));
        channel.writeAndFlush(rpcRequest)
                .addListener(future -> {
                    if (future.isSuccess()) {
                        long timeTaken = System.currentTimeMillis() - startTime;
                        if (logger.isTraceEnabled()) {
                            logger.trace("Sending request "+requestId+" to "+getRemoteAddress(channel)+" took "+timeTaken+" ms");
                        }
                    } else {
                        String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
                                getRemoteAddress(channel), future.cause());
                        logger.error(errorMsg, future.cause());
                        handler.removeRpcRequest(requestId);
                        channel.close();
                        try {
                            callback.onFailure(new IOException(errorMsg, future.cause()));
                        } catch (Exception e) {
                            logger.error("Uncaught exception in RPC response callback handler!", e);
                        }
                    }
                });

        return requestId;
    }

    /**
     * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
     * a specified timeout for a response.
     */
    public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
        final SettableFuture<ByteBuffer> result = SettableFuture.create();

        sendRpc(message, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                ByteBuffer copy = ByteBuffer.allocate(response.remaining());
                copy.put(response);
                // flip "copy" to make it readable
                copy.flip();
                result.set(copy);
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });

        try {
            return result.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
     * message, and no delivery guarantees are made.
     *
     * @param message The message to send.
     */
    public void send(ByteBuffer message) {
        channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
    }

    /**
     * Removes any state associated with the given RPC.
     *
     * @param requestId The RPC id returned by {@link #sendRpc(ByteBuffer, RpcResponseCallback)}.
     */
    public void removeRpcRequest(long requestId) {
        handler.removeRpcRequest(requestId);
    }

    /** Mark this channel as having timed out. */
    public void timeOut() {
        this.timedOut = true;
    }

    @VisibleForTesting
    public NettyChannelResponseHandler getHandler() {
        return handler;
    }

    @Override
    public void close() {
        // close is a local operation and should finish with milliseconds; timeout just to be safe
        channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("remoteAdress", channel.remoteAddress())
                .add("clientId", clientId)
                .add("isActive", isActive())
                .toString();
    }
}
