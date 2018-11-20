package com.example.srpc.nettyrpc.netty

import java.io.{IOException, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeoutException}
import java.util.concurrent.atomic.AtomicBoolean

import com.example.jrpc.nettyrpc.exception.{RpcEndpointNotFoundException, RpcException, RpcTimeoutException}
import com.example.jrpc.nettyrpc.netty.{NettyClientFactory, NettyContext, NettyServer}
import com.example.jrpc.nettyrpc.netty.handler.client.ConcurrentNettyChannelClient
import com.example.jrpc.nettyrpc.netty.handler.server.INettyServerBootstrapWrapper
import com.example.jrpc.nettyrpc.rpc.{EndpointAddress, HostPort, RpcEnvConfig}
import com.example.srpc.nettyrpc.serde.RpcSerializer
import com.example.srpc.nettyrpc._
import com.example.srpc.nettyrpc.message._
import com.example.srpc.nettyrpc.serde.ByteBufferOutputStream
import com.example.srpc.nettyrpc.util.ThreadUtils
import com.google.common.util.concurrent.MoreExecutors
import org.apache.commons.logging.LogFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag

/**
  * Created by yilong on 2018/3/18.
  * NettyRpcEvn 是 RpcServer 和 RpcClient 所有处理的入口
  * 1，不同的 Endpoint 在 server 通过 NettyRpcEnv 注册自身 ，从而处理对应模块的 RPC 消息
  * 2，在 Client 通过 NettyRpcEnv 获取到 EndpointRef , 通过 EndpointRef 的 Send 接口
  *    发送消息给目标 Endpoint
  *    EndpointRef 可以在 Client 用于多线程场景
  */
class NettyRpcEnv(val config: RpcEnvConfig) extends RpcEnv(config) {
  private val logger = LogFactory.getLog(classOf[NettyRpcEnv])

  val rpcSerializer = new RpcSerializer

  private val outboxes = new ConcurrentHashMap[HostPort, Outbox]()
  private val stopped = new AtomicBoolean(false)

  private val sameThreadExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(config.getRpcConfig.getServerThreads * 3)/*(MoreExecutors.newDirectExecutorService()*/)

  val timeoutScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("NettyRpcEnvTimeout")

  val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection", config.getRpcConfig.getConnectTaskThreads)

  // 每个 NettyRpcEnv 包含一个 消息分发处理器（dispatcher）和 一个通用 Rpc Callback（NettyRpcHandler）
  //
  val dispatcher = new RpcDispatcher(this)
  val nettyRpcHandler = new NettyRpcHandler(this, dispatcher)
  val nettyContext = new NettyContext(config.getRpcConfig, nettyRpcHandler)
  val clientFactory = new NettyClientFactory(config.getRpcConfig,
                            nettyContext,
                            Collections.emptyList())
  var server : NettyServer = null

  /**
    * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
    * [[ RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
    */
  //override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
  //
  //}

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
    */
  override def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = EndpointAddress.parse(uri)
    val endpointRef = new NettyRpcEndpointRef(config, addr, this)
    val verifyRef = new NettyRpcEndpointRef(config,
      new EndpointAddress(addr.getHostPort, RpcEndpointVerifier.NAME), this)
    val checkExistence = RpcEndpointVerifier.CheckExistence(addr.getName)
    verifyRef.ask[Boolean](checkExistence).flatMap({find => {
      if (find) Future.successful(endpointRef)
      else Future.failed(new RpcEndpointNotFoundException(addr.getName))
    }})(sameThreadExecutionContext)
  }

  /**
    * Stop [[RpcEndpoint]] specified by `endpoint`.
    */
  override def stop(endpoint: String): Unit = {
    dispatcher.stop(endpoint)
  }

  /**
    * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
    * guarantee thread-safety.
    */
  override def setupEndpoint(name: String, endpoint: RpcEndpoint): EndpointAddress = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  /**
    * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
    * call [[awaitTermination()]] straight after [[shutdown()]].
    */
  override def shutdown(): Unit = {
    cleanup()
  }

  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    val iter = outboxes.values().iterator()
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.receiver)
      outbox.stop()
    }

    if (timeoutScheduler != null) {
      timeoutScheduler.shutdownNow()
    }

    if (dispatcher != null) {
      dispatcher.stop()
    }

    if (server != null) {
      server.close()
    }

    if (clientFactory != null) {
      clientFactory.close()
    }

    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }
  }

  /**
    * Wait until [[RpcEnv]] exits.
    * TODO do we need a timeout parameter?
    */
  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  /**
    * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
    * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
    */
  //override def deserialize[T](deserializationAction: () => T): T = {
  //
  //}

  override def serialize(content: Any): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = new ObjectOutputStream(bos)
    try {
      out.writeObject(content)
    } finally {
      out.flush()
      out.close()
    }

    ByteBuffer.wrap(bos.toByteArray, 0, bos.toByteArray.length)
  }

  def startServer(bindAddress: String, port: Int): Unit = {
    //TODO:
    val bootstraps: java.util.List[INettyServerBootstrapWrapper] = java.util.Collections.emptyList()
    //创建并开始 netty server
    server = nettyContext.createServer(bindAddress, port, bootstraps)
    //注册一个缺省的 Endpoint (RpcEndpointVerifier) , 校验 client 希望连接的 Endpoint 是否已经存在
    dispatcher.registerRpcEndpoint(RpcEndpointVerifier.NAME,
      new RpcEndpointVerifier(this, RpcEndpointVerifier.NAME, dispatcher))
  }

  /**
    * Return the address that [[RpcEnv]] is listening to.
    */
  override def address: HostPort = {
    server match {
      case null => null
      case _ => new HostPort(config.getHost, config.getPort)
    }
  }

  def ask[T: ClassTag](message: RpcRequestMessage, timeout: Long): Future[T] = {
    val promise = Promise[Any]

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        logger.warn(s"Ignored failure: $e")
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcResponseFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          logger.warn(s"Ignored message: $reply")
        }
    }

    def onSuccssCallback(client: ConcurrentNettyChannelClient, rsp : ByteBuffer) : Unit = {
      logger.info("onSuccssCallback : "+ client.toString)
      var obj : Any = null;
      try {
        obj = rpcSerializer.deserialize(rsp)
      } catch {
        case ioe : IOException => {
          logger.error(ioe.getMessage, ioe)
        }
        case cnf : ClassNotFoundException => {
          logger.error(cnf.getMessage, cnf)
        }
      }
      if (obj != null) {
        onSuccess(obj)
      } else {
        onFailure(new RpcException("onSuccssCallback deserialize failed!"))
      }
    }

    val rpcOutMsg = new RpcOutboxMessage(message.serialize(this),
      onFailure,
      //定义Rpc消息成功响应时候， Future 执行调用的函数
      onSuccssCallback)

    postToOutbox(message.receiver.getHostPort, rpcOutMsg)

    //在 Future 失败的时候，执行调用的函数，只关注 超时导致的失败
    promise.future.onFailure{
      case _ : TimeoutException => rpcOutMsg.onTimeout()
      case _ =>
    }(sameThreadExecutionContext)

    //定义超时任务，在 Rpc 消息没有相应的时候 ，保证 Future 可以正常结束
    val timeoutCancel = timeoutScheduler.schedule(new Runnable {
      override def run(): Unit = {
        onFailure(new TimeoutException("Future Timeout Cancel "))
      }
    }, timeout, java.util.concurrent.TimeUnit.MILLISECONDS)

    //在 Future 完成（Failure or  Success）的时候，取消定时器
    //如果是定时器超时导致的失败，这里的定时器取消操作相当于 do nothing
    promise.future.onComplete{
      v => timeoutCancel.cancel(true)
    }(sameThreadExecutionContext)

    //返回 promis.future ， 即一个 Future 类型的对象
    //mapTo 把一个 Any 类型转变为 T 类型
    promise.future.mapTo[T].recover({
      case e1 : RpcTimeoutException => {
        logger.warn(e1.getMessage, e1)
        throw e1
      }
      case e2 : TimeoutException => {
        logger.warn(e2.getLocalizedMessage, e2)
        throw new RpcTimeoutException(e2.getMessage)
      }
    })(sameThreadExecutionContext)
  }

  private def postToOutbox(receiver: HostPort, message: OutboxMessage): Unit = {
    val targetOutbox = {
      val outbox = outboxes.get(receiver)
      if (outbox == null) {
        val newOutbox = new Outbox(this, receiver)
        val oldOutbox = outboxes.putIfAbsent(receiver, newOutbox)
        if (oldOutbox == null) {
          newOutbox
        } else {
          oldOutbox
        }
      } else {
        outbox
      }
    }

    if (stopped.get) {
      // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
      outboxes.remove(receiver)
      targetOutbox.stop()
    } else {
      targetOutbox.send(message)
    }
  }

  def removeOutbox(address: HostPort): Unit = {
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }
}
