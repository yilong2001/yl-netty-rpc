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

package com.example.jrpc.nettyrpc.netty.message.req;

import com.example.jrpc.nettyrpc.netty.buffer.ManagedBuffer;
import com.example.jrpc.nettyrpc.netty.buffer.NettyManagedBuffer;
import com.example.jrpc.nettyrpc.netty.message.IMessage;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * A generic RPC which is handled by a remote {@link com.example.jrpc.jrpcsnettyrpc.netty.NettyRpcHandler}.
 * This will correspond to a single
 * {@link com.example.jrpc.nettyrpc.netty.message.rsp.ResponseMessage} (either success or failure).
 */
public final class RpcRequest extends RequestMessage {
  /** Used to link an RPC request with its response. */
  public final long requestId;

  public RpcRequest(long requestId, ManagedBuffer message) {
    super(message, true);
    this.requestId = requestId;
  }

  @Override
  public MessageType type() { return IMessage.MessageType.RpcRequest; }

  @Override
  public int encodedLength() {
    // The integer (a.k.a. the body size) is not really used, since that information is already
    // encoded in the frame length. But this maintains backwards compatibility with versions of
    // RpcRequest that use Encoders.ByteArrays.
    return 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    // 请求数据编码格式：[reqId],[data length], [data]
    // 请求data 数据放在 Message Body，而不是合在一起，是为了 zero-copy

    buf.writeLong(requestId);
    // See comment in encodedLength().
    buf.writeInt((int) body().size());
  }

  public static RpcRequest decode(ByteBuf buf) {
    long requestId = buf.readLong();
    // See comment in encodedLength().
    buf.readInt();
    return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcRequest) {
      RpcRequest o = (RpcRequest) other;
      return requestId == o.requestId && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("requestId", requestId)
      .add("body", body())
      .toString();
  }
}
