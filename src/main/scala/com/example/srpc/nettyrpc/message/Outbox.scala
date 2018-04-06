package com.example.srpc.nettyrpc.message

import java.util
import java.util.concurrent.Future
import javax.annotation.concurrent.GuardedBy

import com.example.jrpc.nettyrpc.exception.RpcException
import com.example.jrpc.nettyrpc.netty.handler.client.ConcurrentNettyChannelClient
import com.example.jrpc.nettyrpc.rpc.HostPort
import com.example.srpc.nettyrpc.netty.NettyRpcEnv
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal

/**
  * Created by yilong on 2018/3/28.
  */
class Outbox(val nettyRpcEnv: NettyRpcEnv, val receiver: HostPort) {
  private val logger = LoggerFactory.getLogger(classOf[Outbox])

  val outbox = this

  @GuardedBy("this")
  val messageQueue = new util.LinkedList[OutboxMessage]()

  @GuardedBy("this")
  var client : ConcurrentNettyChannelClient = _

  @GuardedBy("this")
  var connectFuture : Future[_] = _

  @GuardedBy("this")
  private var stopped = false

  @GuardedBy("this")
  private var draining = false

  def send(message: OutboxMessage) : Unit = {
    val isStop = synchronized(
      if (stopped) {
        stopped
      } else {
        messageQueue.add(message)
        false
      }
    )

    if (isStop) {
      message.onFailure(new RpcException("outbox is stopped"))
    } else {
      drainOutbox();
    }
  }

  def closeClient(): Unit = {
    client = null
  }

  def drainOutbox() : Unit = {
    var message : OutboxMessage = null

    synchronized({
      if (stopped) {
        logger.info("drainOutbox : stopped")
        return
      }

      if (connectFuture != null) {
        logger.info("drainOutbox : connectFuture is not null, client is connection...")
        return
      }

      if (client == null) {
        launchConnectTask();
        logger.info("drainOutbox : client is null")
        return
      }

      //only support single thread draing
      if (draining) {
        logger.info("drainOutbox : now is draining")
        return
      }

      message = messageQueue.poll()
      if (message == null) {
        logger.info("drainOutbox : message queue is empty")
        return
      }
      draining = true
    })

    while (true) {
      try {
        val _client = synchronized({
          client
        })
        if (_client != null) {
          message.sendWith(_client)
        } else {
          logger.warn("Outbox drain out box, but client is null")
        }
      } catch {
        case NonFatal(e) => handleNetworkFailure(e)
      }

      synchronized({
        if (stopped) {
          return
        }

        message = messageQueue.poll()
        if (message == null) {
          draining = false
          return
        }
      })
    }
  }

  def stop() : Unit = {
    synchronized({
      if (stopped) {
        return
      }

      stopped = true
      if (connectFuture != null) {
        connectFuture.cancel(true)
        connectFuture = null
      }

      closeClient()
    })

    var message = messageQueue.poll()
    while (message != null) {
      message.onFailure(new RpcException("Message is dropped because Outbox is stopped"))
      message = messageQueue.poll()
    }
    assert(messageQueue.isEmpty)
  }

  def handleNetworkFailure(t : Throwable) : Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
      closeClient()
    }

    nettyRpcEnv.removeOutbox(receiver)

    var message = messageQueue.poll()
    while (message != null) {
      message.onFailure(t)
      message = messageQueue.poll()
    }
    assert(messageQueue.isEmpty)
  }

  def launchConnectTask() : Unit = {
    connectFuture = nettyRpcEnv.clientConnectionExecutor.submit(new Runnable {
      override def run(): Unit = {
        var isClientOk = false
        try {
          val _client = nettyRpcEnv.clientFactory.createClient(receiver.getHost, receiver.getPort)
          outbox.synchronized({
            client = _client
            if (stopped) {
              logger.info("launchConnectTask, but outbox is stopped")
              closeClient()
            }
          })
          isClientOk = true
        } catch {
          case e1: InterruptedException => logger.error(e1.getMessage)
          case e : Exception => {
            logger.error(e.getMessage, e)
            outbox.synchronized {
              connectFuture = null
            }
            handleNetworkFailure(e)
          }
        }

        if (isClientOk) {
          outbox.synchronized {
            connectFuture = null
          }
          drainOutbox()
        }
      }
    })
  }
}
