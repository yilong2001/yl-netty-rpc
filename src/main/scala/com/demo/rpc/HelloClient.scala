package demo.rpc

import java.util.concurrent.TimeUnit

import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcConfig, RpcEnvConfig}
import com.example.srpc.nettyrpc.{RpcEndpointRef, RpcEnv}
import com.example.srpc.nettyrpc.netty.NettyRpcEnvFactory
import com.example.srpc.nettyrpc.util.ThreadUtils
import demo.rpc.HelloServer.serverName

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.Random

/**
  * Created by yilong on 2018/4/6.
  */
object HelloClient {
  case class SayHi(msg: String)

  case class SayBye(msg: String)

  val rd = new Random(System.currentTimeMillis())

  def main(args: Array[String]): Unit = {
    syncCall()
  }

  def syncCall() = {
    val threadPool = ThreadUtils.newDaemonCachedThreadPool("pool", 100)

    val rpcEnv: RpcEnv = RpcEnv.create(new RpcConfig(), "localhost", 9091, true)

    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(new HostPort("localhost", 9091),
      HelloServer.serverName)

    (0 to 1000).foreach(id => {
      threadPool.submit(new Runnable {
        override def run(): Unit = {
          val result = endPointRef.askSync[String](SayBye("no:"+id), 1000*10)
          println(id + " : " + result)
        }
      })
    })

    threadPool.awaitTermination(1000*20, TimeUnit.MILLISECONDS)
    threadPool.shutdownNow()
    println("********** end *************")
  }
}
