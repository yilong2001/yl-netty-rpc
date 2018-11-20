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

import com.example.jrpc.nettyrpc.netty.handler.INettyChannelHandler;
import com.example.jrpc.nettyrpc.netty.handler.NettyChannelCommonHandler;
import com.example.jrpc.nettyrpc.netty.message.rsp.ResponseMessage;
import com.example.jrpc.nettyrpc.netty.message.rsp.RpcFailure;
import com.example.jrpc.nettyrpc.netty.message.rsp.RpcResponse;
import com.example.jrpc.nettyrpc.rpc.RpcResponseCallback;
import io.netty.channel.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.Predef;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.example.jrpc.nettyrpc.netty.handler.NettyChannelCommonHandler.getRemoteAddress;

/**
 * Created by yilong on 2018/3/11.
 *  为了易于理解，在 TransportResponseHandler 基础上做了简化
 * ，简化了对 Block 和 Stream 的支持，仅仅保留对 RpcMessage 的处理
 */
public class NettyChannelResponseHandler extends INettyChannelHandler<ResponseMessage> {
    private static final Log logger = LogFactory.getLog(NettyChannelResponseHandler.class);

    private final Channel channel;

    private final Map<Long, RpcResponseCallback> outstandingRpcs;

    /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
    private final AtomicLong timeOfLastRequestNs;

    public NettyChannelResponseHandler(Channel channel) {
        this.channel = channel;
        this.outstandingRpcs = new ConcurrentHashMap<Long, RpcResponseCallback>();
        this.timeOfLastRequestNs = new AtomicLong(0);
    }

    @Override
    public void handle(ResponseMessage message) throws Exception {
        // 做为示例，简化处理 ： 仅处理 RpcResponse 和 RpcFailure
        if (message instanceof RpcResponse) {
            RpcResponse resp = (RpcResponse) message;
            RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
            if (listener == null) {
                logger.warn(String.format("Ignoring response for RPC %d from %s (%d bytes) since it is not outstanding",
                        resp.requestId, getRemoteAddress(channel), resp.body().size()));
            } else {
                outstandingRpcs.remove(resp.requestId);
                try {
                    listener.onSuccess(resp.body().nioByteBuffer());
                } finally {
                    resp.body().release();
                }
            }
        } else if (message instanceof RpcFailure) {
            RpcFailure resp = (RpcFailure) message;
            RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
            if (listener == null) {
                logger.warn(String.format("Ignoring response for RPC {%d} from {%s} ({%s}) since it is not outstanding",
                        resp.requestId, getRemoteAddress(channel), resp.errorString));
            } else {
                outstandingRpcs.remove(resp.requestId);
                listener.onFailure(new RuntimeException(resp.errorString));
            }
        } else {
            throw new IllegalStateException("Unknown response type: " + message.type());
        }
    }

    @Override
    public void channelActive() {
    }

    @Override
    public void channelInactive() {
        if (numOutstandingRequests() > 0) {
            String remoteAddress = getRemoteAddress(channel);
            logger.error(String.format("Still have {%d} requests outstanding when connection from {%s} is closed",
                    numOutstandingRequests(), remoteAddress));
            failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
        }
    }

    @Override
    public void exceptionCaught(Throwable cause) {
        if (numOutstandingRequests() > 0) {
            String remoteAddress = getRemoteAddress(channel);
            logger.error(String.format("Still have {%d} requests outstanding when connection from {%s} is closed",
                    numOutstandingRequests(), remoteAddress));
            failOutstandingRequests(cause);
        }
    }

    public void addRpcRequest(long requestId, RpcResponseCallback callback) {
        updateTimeOfLastRequest();
        outstandingRpcs.put(requestId, callback);
    }

    public void removeRpcRequest(long requestId) {
        outstandingRpcs.remove(requestId);
    }

    /**
     * Fire the failure callback for all outstanding requests. This is called when we have an
     * uncaught exception or pre-mature connection termination.
     */
    private void failOutstandingRequests(Throwable cause) {
        for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
            entry.getValue().onFailure(cause);
        }

        // It's OK if new fetches appear, as they will fail immediately.
        outstandingRpcs.clear();
    }

    /** Returns total number of outstanding requests (fetch requests + rpcs) */
    public int numOutstandingRequests() {
        return outstandingRpcs.size();
    }

    /** Returns the time in nanoseconds of when the last request was sent out. */
    public long getTimeOfLastRequestNs() {
        return timeOfLastRequestNs.get();
    }

    /** Updates the time of the last request to the current system time. */
    public void updateTimeOfLastRequest() {
        timeOfLastRequestNs.set(System.nanoTime());
    }
}
