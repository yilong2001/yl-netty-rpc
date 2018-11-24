package com.example.srpc.nettyrpc

import java.util.concurrent._
import javax.annotation.concurrent.GuardedBy

import com.example.jrpc.nettyrpc.exception.{RpcEnvStoppedException, RpcException}
import com.example.jrpc.nettyrpc.rpc.{EndpointAddress, RpcResponseCallback}
import com.example.srpc.nettyrpc.message._
import com.example.srpc.nettyrpc.netty.{NettyRpcEnv, NettyRpcRemoteCallContext}
import com.example.srpc.nettyrpc.util.ThreadUtils
import org.apache.commons.logging.LogFactory

import scala.collection.JavaConverters._


/**
  * Created by yilong on 2018/3/19.
  */
class RpcDispatcher(nettyEnv: NettyRpcEnv) {
  private val logger = LogFactory.getLog(classOf[RpcDispatcher])

  private class EndpointData(val name: String,val endpoint: RpcEndpoint) {
    val addr = new EndpointAddress(nettyEnv.address, name)
    val inbox = new Inbox(addr, endpoint)
    inbox.post(OnStart)
  }

  // 每个 Endpoint 对应一个 inbox
  private val endpointDataMaps: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]

  // 做为一个消息队列，处理 Endpoint
  private val endpointReceivers = new LinkedBlockingQueue[EndpointData]

  private val inboxMessageProcessThreadpool: ThreadPoolExecutor = {
    val numThreads = nettyEnv.config.getRpcConfig.getServerThreads
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          val data = endpointReceivers.poll()
          if (data == null) {
            Thread.`yield`()
          } else {
            try {
              // 停止 inbox 的消息循环，通过 PoisonPill (一个特殊的 EndpointData)
              // 发送给所有消息分发线程
              if (data == PoisonPill) {
                endpointReceivers.offer(PoisonPill)
                return
              }

              if (data == null) {
                logger.error("data is null ... ")
                throw new RuntimeException("data is null ... ")
              }

              if (data.inbox == null) {
                logger.error("data.inbox is null ... ")
                throw new RuntimeException("data.inbox is null ... ")
              }

              if (RpcDispatcher.this == null) {
                logger.error("RpcDispatcher.this is null ... ")
                throw new RuntimeException("RpcDispatcher.this is null ... ")
              }
              data.inbox.process(RpcDispatcher.this)
            } catch {
              case e: Exception => logger.error(e.getMessage, e)
            }
          }
        }
      } catch {
        case e1:Exception => logger.error(e1.getMessage, e1)
      }
    }
  }

  private val PoisonPill = new EndpointData(null, null)

  @GuardedBy("this")
  private var stopped = false

  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): EndpointAddress = {
    val addr = new EndpointAddress(nettyEnv.address, name)
    synchronized {
      logger.info("registerRpcEndpoint start : "+name+"; "+endpoint.epName)
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      if (endpointDataMaps.putIfAbsent(name,
        new EndpointData(name, endpoint)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }
      val data = endpointDataMaps.get(name)
      if (data == null) {
        logger.error("register prc endpoint, but EndpointData is null ... ")
        throw new RuntimeException("register prc endpoint, but EndpointData is null ... ")
      }
      endpointReceivers.offer(data)  // for the OnStart message
    }
    addr
  }

  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    logger.info("unregisterRpcEndpoint start : "+name+"; ")
    val data = endpointDataMaps.remove(name)
    if (data != null) {
      //OnStop message is added when inbox.stop called
      data.inbox.stop()
      endpointReceivers.offer(data)  // for the OnStop message
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
    // TODO: ???
  }

  def stop(rpcEndpointRefName: String): Unit = {
    logger.info("stop : "+rpcEndpointRefName+"; ")
    synchronized {
      if (stopped) {
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      unregisterRpcEndpoint(rpcEndpointRefName)
    }
  }

  def stop(): Unit = {
    synchronized {
      if (stopped) {
        //TODO: ???
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      stopped = true
    }
    endpointDataMaps.keySet().asScala.foreach(unregisterRpcEndpoint)
    endpointReceivers.offer(PoisonPill)
    inboxMessageProcessThreadpool.shutdown()
  }

  def awaitTermination(): Unit = {
    inboxMessageProcessThreadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  def awaitTermination(timeoutMs : Long): Unit = {
    inboxMessageProcessThreadpool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)
  }

  def shutdown(): Unit = {
    inboxMessageProcessThreadpool.shutdown()
  }

  def shutdownNow(): Unit = {
    inboxMessageProcessThreadpool.shutdownNow()
  }

  /**
    * Return if the endpoint exists
    */
  def verify(name: String): Boolean = {
    endpointDataMaps.containsKey(name)
  }

  /**
    * Send a message to all registered [[RpcEndpoint]]s in this process.
    * This can be used to make network events known to all end points (e.g. "a new node connected").
    */
  def postToAll(message: InboxMessage): Unit = {
    val iter = endpointDataMaps.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
      postMessage(name, message, (e) => logger.warn(s"Message $message dropped. ${e.getMessage}"))
    }
  }

  /** Posts a message sent by a remote endpoint. */
  def postRemoteMessage(message: RpcRequestMessage, callback: RpcResponseCallback): Unit = {
    val rpcCallContext =
      new NettyRpcRemoteCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcInboxMessage(message.senderAddress, message.content, rpcCallContext)

    postMessage(message.receiver.getName, rpcMessage, (e) => callback.onFailure(e))
  }

  def postOneWayMessage(message: RpcRequestMessage): Unit = {
    postMessage(message.receiver.getName, OneWayInboxMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  /**
    * Posts a message to a specific endpoint.
    *
    * @param endpointName name of the endpoint.
    * @param message the message to post
    * @param callbackIfStopped callback function if the endpoint is stopped.
    */
  private def postMessage(endpointName: String,
                           message: InboxMessage,
                           callbackIfStopped: (Exception) => Unit): Unit = {
    logger.info("postMessage : "+endpointName+"; ")

    val error = synchronized {
      val data = endpointDataMaps.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (data == null) {
        Some(new RpcException(s"Could not find $endpointName."))
      } else {
        data.inbox.post(message)
        endpointReceivers.offer(data)
        None
      }
    }

    // We don't need to call `onStop` in the `synchronized` block
    error.foreach(callbackIfStopped)
  }
}
