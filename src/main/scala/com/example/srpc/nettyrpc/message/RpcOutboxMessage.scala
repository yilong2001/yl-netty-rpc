package com.example.srpc.nettyrpc.message

import java.nio.ByteBuffer

import com.example.jrpc.nettyrpc.netty.handler.client.ConcurrentNettyChannelClient
import com.example.jrpc.nettyrpc.rpc.RpcResponseCallback
import org.apache.commons.logging.LogFactory

/**
  * Created by yilong on 2018/3/28.
  */
sealed trait OutboxMessage {
  def onFailure(e: Throwable): Unit
  def onTimeout() : Unit
  def sendWith(client: ConcurrentNettyChannelClient) : Unit
}

class RpcOutboxMessage(val content : ByteBuffer,
                       val onFailureCallback: (Throwable) => Unit,
                       val onSuccessCallback: (ConcurrentNettyChannelClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback {
  private val logger = LogFactory.getLog(classOf[RpcOutboxMessage])

  private var client: ConcurrentNettyChannelClient = _
  private var requestId: Long = _

  override def onFailure(e: Throwable): Unit = {
    onFailureCallback(e)
  }

  override def onSuccess(rsp: ByteBuffer): Unit = {
    onSuccessCallback(client, rsp)
  }

  override def onTimeout(): Unit = {
    //
    if (client != null) {
      client.removeRpcRequest(requestId)
    } else {
      logger.warn("RpcOutboxMessage $requestId timeout")
    }
  }

  override def sendWith(client: ConcurrentNettyChannelClient): Unit = {
    this.client = client
    this.requestId = client.sendRpc(content, this)
  }
}
