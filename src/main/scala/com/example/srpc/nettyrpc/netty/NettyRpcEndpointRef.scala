package com.example.srpc.nettyrpc.netty

import com.example.jrpc.nettyrpc.rpc.{EndpointAddress, HostPort, RpcEnvConfig}
import com.example.srpc.nettyrpc.RpcEndpointRef
import com.example.srpc.nettyrpc.message.RpcRequestMessage

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * Created by yilong on 2018/3/27.
  */
class NettyRpcEndpointRef(val conf : RpcEnvConfig,
                          val endpointAddress : EndpointAddress,
                          val nettyRpcEnv : NettyRpcEnv) extends RpcEndpointRef(conf) {
  /**
    * return the address for the [[RpcEndpointRef]]
    */
  override def address: HostPort = endpointAddress.getHostPort

  override def name: String = endpointAddress.getName

  /**
    * Sends a one-way asynchronous message. Fire-and-forget semantics.
    */
  //override def send(message: Any): Unit = {
  //  //TODO:
  //  nettyRpcEnv.
  //}

  /**
    * Send a message to the corresponding [[com.example.snettyrpc.RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
    * receive the reply within the specified timeout.
    *
    * This method only sends the message once and never retries.
    */
  override def ask[T: ClassTag](message: Any, timeout: Long): Future[T] = {
    nettyRpcEnv.ask[T](new RpcRequestMessage(new HostPort(conf.getHost, conf.getPort), endpointAddress, message), timeout)
  }

  override def toString: String = s"NettyRpcEndpointRef(${endpointAddress})"

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => endpointAddress == other.endpointAddress
    case _ => false
  }

  final override def hashCode(): Int =
    if (endpointAddress == null) 0 else endpointAddress.hashCode()
}
