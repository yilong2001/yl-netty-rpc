package com.example.srpc.nettyrpc.netty

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcHandler, RpcResponseCallback}
import com.example.srpc.nettyrpc.RpcDispatcher
import com.example.srpc.nettyrpc.message.{RemoteProcessConnected, RpcRequestMessage}

/**
  * Created by yilong on 2018/3/18.
  */
class NettyRpcHandler(rpcEnv: NettyRpcEnv, dispatcher: RpcDispatcher) extends RpcHandler {
  private val remoteAddresses = new ConcurrentHashMap[HostPort, HostPort]()

  /**
    * 调用位置:
    * com.example.jnettyrpc.netty.handler.server.NettyChannelRequestHandler:
    *                        private Unit processRpcRequest(req: RpcRequest)
    *
    * Receive a single RPC message. Any exception thrown while in this method will be sent back to
    * the client in string form as a standard RPC failure.
    * This method will not be called in parallel for a single TransportClient (i.e., channel).
    * @param client   A channel client which enables the handler to make requests back to the sender
    *                 of this RPC. This will always be the exact same object for a particular channel.
    * @param message  The serialized bytes of the RPC.
    * @param callback Callback which should be invoked exactly once upon success or failure of the
    *                 RPC.
    */

  override def receive(client: HostPort, message: ByteBuffer, callback: RpcResponseCallback): Unit = {
    //TODO: dispatcher.postRemoteMessage()
    val msg = internalReceive(client, message)
    //分发消息 到 对应的 Endpoint

    dispatcher.postRemoteMessage(msg, callback)
  }

  private def internalReceive(client: HostPort, message: ByteBuffer): RpcRequestMessage = {
    assert(client != null)
    //反序列化 RpcRequestMessage
    val requestMessage = RpcRequestMessage(rpcEnv, message)

    assert(requestMessage.senderAddress != null)
    val remoteEnvAddress = requestMessage.senderAddress

    //保存服务端channel获取到的 client address 与 客户端channel获取到的 address 映射关系
    if (remoteAddresses.putIfAbsent(client, remoteEnvAddress) == null) {
      dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
    }

    requestMessage
  }
}
