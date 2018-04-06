# yl-netty-rpc
基于 Spark Netty Rpc 框架，重新实现的一个 Netty Rpc 框架 ( scala + java )

## 说明
 Spark Netty RPC 是一个高效的 RPC 框架，即可用于非 Spark 项目，用来学习研究 Netty & RPC 设计思想也是很好的素材。
 已有不少 Spark RPC 的 git 项目，把 Spark RPC 功能从 Spark 中分离出来。若出于使用，已能基本满足需要。
 不过，如果要学习 Spark Netty RPC，还需要更详细的拆解，以及适当做一些简化以易于理解。
 yl-netty-rpc 参考 Spark Netty RPC ，适当做了简化，80%代码进行重写。可用于RPC，也易于学习和理解。
 yl-netty-rpc 基于 Spark 2.1 ( 使用 netty 代替了 akka ) 开发。

- [1. 协议栈](#1-协议栈)
- [2. 依赖](#2-依赖)
- [3. 实例](#3-如何使用)
  - [3.1 自定义 RPC 服务](#31-)
  - [3.2 运行 server](#32-运行服务)
  - [3.3 使用 Client ](#33-客户端调用)
- [4. 关于 RpcConfig](#4-rpcConfig)


## 1. 协议栈

![protocal](https://github.com/yilong2001/yl-netty-rpc/blob/master/img/SparkNettyRpcProtocalStack.jpg)

### 客户端
- 用户定义的消息对象
- 增加目标 Endpoint 地址
- 使用 [length]-[message type]-[message content] 方式对消息进行编码
- 增加 RequestId, 在 Client 对不同的 Request 以及 Request Callback Function 进行匹配
- 使用 Frame 进行传输 (每个 Frame 固定字节长度)
- 底层使用 TCP/IP 协议，使用 NIO channel

### 服务端
- NIO 接收数据
- 使用 FrameDecode 解码
- 从 Request 提取 RequestId 和 Content 通过 ChannelRequestHandler 处理
- 对 Content 解码，提取 Endpoint 和 消息 Object
- 在 Server 把 Request 消息分发到指定的 Endpoint
- Endpoint 完成 Receive and Reply

## 2. 依赖

maven 和 SBT 的依赖方式， 基于 **scala 2.11**.

Maven:

```
<dependency>
    <groupId>com.example.rpc</groupId>
    <artifactId>yl-netty-rpc_2.11</artifactId>
    <version>1.0.0</version>
</dependency>
```

SBT:

```
"com.example.rpc" % "yl-netty-rpc_2.11" % "1.0.0"
```

## 3. 示例

下面给出了一个简单的使用示例

### 3.1 自定义RPC service (endpoint)

一个 endpoint 即为一个 RPC 服务，可定义任意类型的消息对象。不像 thrift，消息对象不需单独编译。

```
class HelloEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {
  override def onStart(): Unit = {
    println("start hello endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi(msg) => {
      //println(s"receive $msg")
      context.reply(s"$msg")
    }
  }

  override def onStop(): Unit = {
    println("stop hello endpoint")
  }

  override val epName: String = "HelloServer"
}

```

`Endpoint` 的核心是实现如下两个函数:

```
  /**
   * 只接收，不需要响应
   */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new RpcException(self + " does not implement 'receive'")
  }

  /**
   * 接收并响应
   */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new RpcException(self + " won't reply anything"))
  }
```


### 3.2 启动服务

要开始RPC服务，实现 HelloWorld 功能，需要如下步骤：

1. create `RpcConfig`, 定义netty 和 服务端运行的必要参数
2. create `RpcEnv` (RpcConfig, host, port, clientMode), 服务启动、及服务所需环境信息都依赖 RpcEnv
3. create `HelloEndpoint` , 并调用 setupEndpoint 注册此 endpoint 服务
4. `awaitTermination` 使得 server 驻留在 jvm 中运行.

```
import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcConfig}
import com.example.srpc.nettyrpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

object HelloServer {
  val serverName = "helloServer";

  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).asInstance[Int]

    val rpcEnv: RpcEnv = RpcEnv.create(new RpcConfig(), host, port, false)

    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)

    rpcEnv.setupEndpoint(serverName, helloEndpoint)
    rpcEnv.awaitTermination()
  }
}
```

### 3.3 Client call

#### 3.3.1 异步调用

同样的，首先创建 `RpcEnv`；接着，使用 `setupEndpointRef` 获取服务端 `Endpoint` 的本地代理，使用 `EndpointRef` 与 `Endpoint` 通信

```
import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcConfig}
import com.example.srpc.nettyrpc.{RpcEndpointRef, RpcEnv}

object HelloClient {
  case class SayHi(msg: String)

  def main(args: Array[String]): Unit = {
    asyncCall(args(0), args(1).asInstance[Int])
  }

  def asyncCall(host:String, port:Int) = {
    val rpcEnv: RpcEnv = RpcEnv.create(new RpcConfig(), host, port, true)

    val epRef: RpcEndpointRef = rpcEnv.setupEndpointRef(new HostPort(host, port),
      HelloServer.serverName)

    val result: Future[String] = epRef.ask[String](SayHi("world"))
    result.onComplete{ ... }
    Await.result(result, Duration.apply("10s"))
  }
}
```

#### 3.3.2 同步调用

使用 askSync 代替 ask 即可实现同步调用

```
object HelloClient {
  def main(args: Array[String]): Unit = {
    syncCall(args(0), args(1).asInstance[Int])
  }

  def syncCall(host:String, port:Int) = {
    val rpcEnv: RpcEnv = RpcEnv.create(new RpcConfig(), host, port, true)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(new HostPort(host, port),
      HelloServer.serverName)

    val result = endPointRef.askSync[String](SayHi("world"), 1000*100)
    println(result)
  }
}
```

## 4. RpcConfig

`RpcConfig` 是简化版的 `SparkConf` , 定义 `server`、`netty`、`RPC call`所必须的参数

```
val rpcConf = new RpcConfig()
rpcConf.set("spark.rpc.lookupTimeout", "2s")
//or, change params with set function
rpcConf.getRpcConfig.setClientThreads(10)
```

