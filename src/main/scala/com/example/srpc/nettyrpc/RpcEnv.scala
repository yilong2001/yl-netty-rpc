package com.example.srpc.nettyrpc

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.example.jrpc.nettyrpc.rpc.{EndpointAddress, HostPort, RpcConfig, RpcEnvConfig}
import com.example.srpc.nettyrpc.netty.NettyRpcEnvFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by yilong on 2018/3/18.
  */

object RpcEnv {
  def create(conf: RpcConfig,
              bindAddress: String,
              port: Int,
              clientMode: Boolean): RpcEnv = {
    val config = new RpcEnvConfig(conf, bindAddress, port, clientMode)
    new NettyRpcEnvFactory().create(config)
  }
}

abstract class RpcEnv(rpcEnvConfig : RpcEnvConfig) {
  /**
    * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
    * [[ RpcEndpoint.self ]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
    */
  //def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Return the address that [[RpcEnv]] is listening to.
    */
  def address: HostPort

  /**
    * Register a [[RpcEndpoint]] with a name and return its [[EndpointAddress]]. [[RpcEnv]] does not
    * guarantee thread-safety.
    */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): EndpointAddress

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
    */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
    * This is a blocking action.
    */
  def setupEndpointRef(address: HostPort, endpointName: String): RpcEndpointRef = {
    val addr = new EndpointAddress(address, endpointName)
    Await.result(asyncSetupEndpointRefByURI(addr.toString),
      Duration(1000*1000, TimeUnit.MILLISECONDS)) //rpcEnvConfig.getRpcConfig.getRpcAwaitTimeoutMs
  }

  /**
    * Stop [[RpcEndpoint]] specified by `endpoint`.
    */
  def stop(endpointRefName: String): Unit

  /**
    * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
    * call [[awaitTermination()]] straight after [[shutdown()]].
    */
  def shutdown(): Unit

  /**
    * Wait until [[RpcEnv]] exits.
    *
    * TODO do we need a timeout parameter?
    */
  def awaitTermination(): Unit

  /**
    * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
    * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
    */
  //def deserialize[T](deserializationAction: () => T): T

  def serialize(content: Any): ByteBuffer
}
