package com.example.srpc.nettyrpc.util

import java.util.concurrent._

import com.example.jrpc.nettyrpc.exception.RpcException
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.Awaitable
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
  * Created by yilong on 2018/3/29.
  */
object ThreadUtils {
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  def newDaemonCachedThreadPool(prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
      maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  /**
    * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
    * unique, sequentially assigned integer.
    */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  @throws(classOf[RpcException])
  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T = {
    try {
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.result(atMost)(awaitPermission)
    } catch {
      // TimeoutException is thrown in the current thread, so not need to warp the exception.
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new RpcException("Exception thrown in awaitResult: ", t)
    }
  }


  // scalastyle:off awaitready
  /**
    * Preferred alternative to `Await.ready()`.
    *
    * @see [[awaitResult]]
    */
  @throws(classOf[RpcException])
  def awaitReady[T](awaitable: Awaitable[T], atMost: Duration): awaitable.type = {
    try {
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.ready(atMost)(awaitPermission)
    } catch {
      // TimeoutException is thrown in the current thread, so not need to warp the exception.
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new RpcException("Exception thrown in awaitResult: ", t)
    }
  }
}
