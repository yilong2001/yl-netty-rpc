package demo.rpc

import com.example.jrpc.nettyrpc.rpc.{RpcConfig, RpcEnvConfig}
import com.example.srpc.nettyrpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import com.example.srpc.nettyrpc.netty.NettyRpcEnvFactory
import demo.rpc.HelloClient.{SayBye, SayHi}

/**
  * Created by yilong on 2018/4/6.
  */
object HelloServer {
  val serverName = "hello-server";

  def main(args: Array[String]): Unit = {
    val host = "localhost"//args(0)

    val rpcEnv: RpcEnv = RpcEnv.create(new RpcConfig(), host, 9091, false)

    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)

    rpcEnv.setupEndpoint(serverName, helloEndpoint)
    rpcEnv.awaitTermination()
  }
}

class HelloEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {
  override def onStart(): Unit = {
    println("start hello endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi(msg) => {
      //println(s"receive $msg")
      context.reply(s"$msg")
    }
    case SayBye(msg) => {
      //println(s"receive $msg")
      context.reply(s"bye, $msg")
    }
  }

  override def onStop(): Unit = {
    println("stop hello endpoint")
  }

  override val epName: String = HelloServer.serverName
}
