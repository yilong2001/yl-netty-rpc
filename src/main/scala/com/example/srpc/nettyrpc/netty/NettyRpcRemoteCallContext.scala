package com.example.srpc.nettyrpc.netty

import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcResponseCallback}
import com.example.srpc.nettyrpc.RpcCallContext
import com.example.srpc.nettyrpc.message.RpcResponseFailure

/**
  * Created by yilong on 2018/4/4.
  */
abstract class NettyRpcCallContext(override val senderAddress: HostPort)
  extends RpcCallContext {

  protected def send(message: Any): Unit

  override def reply(response: Any): Unit = {
    send(response)
  }

  override def sendFailure(e: Throwable): Unit = {
    send(RpcResponseFailure(e))
  }
}

class NettyRpcRemoteCallContext (nettyEnv: NettyRpcEnv,
                                 callback: RpcResponseCallback,
                                 senderAddress: HostPort)
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    val reply = nettyEnv.serialize(message)
    callback.onSuccess(reply)
  }
}

