package com.example.srpc.nettyrpc

import com.example.jrpc.nettyrpc.rpc.HostPort

/**
  * Created by yilong on 2018/3/19.
  */
trait RpcCallContext {
  /**
    * Reply a message to the sender. If the sender is [[RpcEndpoint]], its [[RpcEndpoint.receive]]
    * will be called.
    */
  def reply(response: Any): Unit

  /**
    * Report a failure to the sender.
    */
  def sendFailure(e: Throwable): Unit

  /**
    * The sender of this message.
    */
  def senderAddress: HostPort
}
