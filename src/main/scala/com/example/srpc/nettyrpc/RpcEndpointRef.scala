package com.example.srpc.nettyrpc

import com.example.jrpc.nettyrpc.exception.RpcException
import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcEnvConfig}

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

/**
  * Created by yilong on 2018/3/18.
  */
abstract class RpcEndpointRef(conf : RpcEnvConfig) extends Serializable {

  private[this] val maxRetries = conf.getRpcConfig.getMaxRetries;
  private[this] val retryWaitMs = conf.getRpcConfig.getRetryWaitMs
  private[this] val defaultAskTimeout = conf.getRpcConfig.getAskTimeoutMs

  /**
    * return the address for the [[RpcEndpointRef]]
    *
    */
  //TODO:
  def address: HostPort

  def name: String

  /**
    * Sends a one-way asynchronous message. Fire-and-forget semantics.
    */
  //def send(message: Any): Unit

  /**
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
    * receive the reply within the specified timeout.
    *
    * This method only sends the message once and never retries.
    */
  def ask[T: ClassTag](message: Any, timeout: Long): Future[T]

  /**
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
    * receive the reply within a default timeout.
    *
    * This method only sends the message once and never retries.
    */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
    * default timeout, throw an exception if this fails.
    *
    * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
    * loop of [[RpcEndpoint]].

    * @param message the message to send
    * @tparam T type of the reply message
    * @return the reply message from the corresponding [[RpcEndpoint]]
    */
  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)

  /**
    * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
    * specified timeout, throw an exception if this fails.
    *
    * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
    * loop of [[RpcEndpoint]].
    *
    * @param message the message to send
    * @param timeout the timeout duration
    * @tparam T type of the reply message
    * @return the reply message from the corresponding [[RpcEndpoint]]
    */
  def askSync[T: ClassTag](message: Any, timeout: Long): T = {
    val future = ask[T](message, timeout)
    awaitResult(future, timeout)
  }

  def awaitResult[T](future: Future[T], duration: Long): T = {
    try {
      val da = Duration.apply(duration, java.util.concurrent.TimeUnit.MILLISECONDS)
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      future.result(da)(awaitPermission)
    } catch {
      case t : Throwable => throw new RpcException("Exception thrown in awaitResult: ", t)
    }
  }
}
