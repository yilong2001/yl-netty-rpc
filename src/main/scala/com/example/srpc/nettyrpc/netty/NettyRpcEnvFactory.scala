package com.example.srpc.nettyrpc.netty

import com.example.jrpc.nettyrpc.rpc.RpcEnvConfig
import com.example.srpc.nettyrpc.RpcEnv

import scala.util.control.NonFatal

/**
  * Created by yilong on 2018/3/18.
  */
class NettyRpcEnvFactory {
  def create(config: RpcEnvConfig): RpcEnv = {
    val nettyEnv = new NettyRpcEnv(config)
    if (!config.isClient) {
      try {
        val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
          nettyEnv.startServer(config.getHost, actualPort)
          (nettyEnv, nettyEnv.address.getPort)
        }

        startNettyRpcEnv(config.getPort)
      }catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
}
