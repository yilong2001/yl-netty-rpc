package com.example.srpc.nettyrpc

import com.example.jrpc.nettyrpc.exception.RpcException
import com.example.jrpc.nettyrpc.rpc.HostPort

/**
  * Created by yilong on 2018/3/18.
  */
trait RpcEndpoint {
  /**
    * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
    */
  val rpcEnv: RpcEnv
  val epName : String

  /**
    * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
    * called. And `self` will become `null` when `onStop` is called.
    *
    * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
    * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
    */
  //final def self: RpcEndpointRef = {
  //  //TODO:
  //  require(rpcEnv != null, "rpcEnv has not been initialized")
  //  rpcEnv.endpointRef(this)
  //}

  /**
    * Process messages from `RpcEndpointRef.send` or `RpcCallContext.reply`. If receiving a
    * unmatched message, `SparkException` will be thrown and sent to `onError`.
    */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new RpcException(rpcEnv.address.toString + " does not implement 'receive'")
  }

  /**
    * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
    * `SparkException` will be thrown and sent to `onError`.
    */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new RpcException(rpcEnv.address.toString + " won't reply anything"))
  }

  /**
    * Invoked when any exception is thrown during handling messages.
    */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
    * Invoked when `remoteAddress` is connected to the current node.
    */
  def onConnected(remoteAddress: HostPort): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked when `remoteAddress` is lost.
    */
  def onDisconnected(remoteAddress: HostPort): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked when some network error happens in the connection between the current node and
    * `remoteAddress`.
    */
  def onNetworkError(cause: Throwable, remoteAddress: HostPort): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked before [[RpcEndpoint]] starts to handle any message.
    */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
    * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
    * use it to send or ask messages.
    */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
    * A convenient method to stop [[RpcEndpoint]].
    */
  final def stop(): Unit = {
    //val _self = self
    //if (_self != null) {
    //  rpcEnv.stop(_self)
    //}
    if (rpcEnv != null) {
      rpcEnv.stop(epName)
    }
  }
}

/**
  * A trait that requires RpcEnv thread-safely sending messages to it.
  *
  * Thread-safety means processing of one message happens before processing of the next message by
  * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
  * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
  * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
  *
  * However, there is no guarantee that the same thread will be executing the same
  * [[ThreadSafeRpcEndpoint]] for different messages.
  */
trait ThreadSafeRpcEndpoint extends RpcEndpoint
