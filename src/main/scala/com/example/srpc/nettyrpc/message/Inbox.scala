package com.example.srpc.nettyrpc.message

import javax.annotation.concurrent.GuardedBy

import com.example.jrpc.nettyrpc.exception.RpcException
import com.example.jrpc.nettyrpc.rpc.{EndpointAddress, HostPort}
import com.example.srpc.nettyrpc._
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/**
  * Created by yilong on 2018/3/19.
  */
sealed trait InboxMessage

case class OneWayInboxMessage(senderAddress: HostPort,
                                        content: Any) extends InboxMessage

case class RpcInboxMessage(senderAddress: HostPort,
                           content: Any,
                           context: RpcCallContext) extends InboxMessage

case object OnStart extends InboxMessage

case object OnStop extends InboxMessage

/** A message to tell all endpoints that a remote process has connected. */
case class RemoteProcessConnected(remoteAddress: HostPort) extends InboxMessage

/** A message to tell all endpoints that a remote process has disconnected. */
case class RemoteProcessDisconnected(remoteAddress: HostPort) extends InboxMessage

/** A message to tell all endpoints that a network error has happened. */
case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: HostPort)
  extends InboxMessage

/**
  * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
  */
class Inbox(val endpointAddr: EndpointAddress, val endpoint: RpcEndpoint) {
  inbox =>  // Give this an alias so we can use it more clearly in closures.

  private val logger = LoggerFactory.getLogger(classOf[Inbox])

  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  /** True if the inbox (and its associated endpoint) is stopped. */
  @GuardedBy("this")
  private var stopped = false

  /** Allow multiple threads to process messages at the same time. */
  @GuardedBy("this")
  private var enableConcurrent = false

  /** The number of threads processing messages for this inbox. */
  @GuardedBy("this")
  private var numActiveThreads = 0

  // ****** call add OnStart message at dispather when new InBox is created ******
  // OnStart should be the first message to process
  //inbox.synchronized {
  //  messages.add(OnStart)
  //}

  /**
    * Process stored messages.
    */
  def process(dispatcher: RpcDispatcher): Unit = {
    var message: InboxMessage = null
    inbox.synchronized {
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      message = messages.poll()
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    while (true) {
      safelyCall(endpoint) {
        message match {
          case RpcInboxMessage(_sender, content, context) =>
            try {
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new RpcException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case NonFatal(e) =>
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayInboxMessage(_sender, content) =>
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new RpcException(s"Unsupported message $message from ${_sender}")
            })

          case OnStart =>
            endpoint.onStart()
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }

          case OnStop =>
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            //TODO: endpointRef ???
            //dispatcher.removeRpcEndpointRef(endpoint)
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      inbox.synchronized {
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          numActiveThreads -= 1
          return
        }
        message = messages.poll()
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }

  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      onDrop(message)
    } else {
      messages.add(message)
      false
    }
  }

  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    if (!stopped) {
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      enableConcurrent = false
      stopped = true
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  /**
    * Called when we are dropping a message. TestSerObject cases override this to test message dropping.
    * Exposed for testing.
    */
  protected def onDrop(message: InboxMessage): Unit = {
    logger.warn(s"Drop $message because $endpointAddr is stopped")
  }

  /**
    * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
    */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try action catch {
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) => logger.error(s"Ignoring error", ee)
        }
    }
  }

}
