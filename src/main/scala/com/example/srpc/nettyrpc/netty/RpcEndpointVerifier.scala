package com.example.srpc.nettyrpc.netty

import com.example.srpc.nettyrpc.{RpcCallContext, RpcDispatcher, RpcEndpoint, RpcEnv}

/**
  * Created by yilong on 2018/4/5.
  */
class RpcEndpointVerifier(override val rpcEnv: RpcEnv,
                          override val epName: String,
                          dispatcher: RpcDispatcher)
  extends RpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RpcEndpointVerifier.CheckExistence(name) => context.reply(dispatcher.verify(name))
  }
}

private[netty] object RpcEndpointVerifier {
  val NAME = "endpoint-verifier"

  /** A message used to ask the remote [[RpcEndpointVerifier]] if an `RpcEndpoint` exists. */
  case class CheckExistence(name: String)
}
