package com.example.srpc.nettyrpc.message

import java.io._
import java.nio.ByteBuffer

import com.example.jrpc.nettyrpc.rpc.{EndpointAddress, HostPort}
import com.example.srpc.nettyrpc.netty.NettyRpcEnv
import com.example.srpc.nettyrpc.serde.{ByteBufferInputStream, ByteBufferOutputStream}

/**
  * Created by yilong on 2018/3/19.
  */
class RpcRequestMessage(val senderAddress: HostPort,
                        val receiver: EndpointAddress,
                        val content: Any) {

  /** Manually serialize [[RpcRequestMessage]] to minimize the size. */
  def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = new ObjectOutputStream(bos)
    try {
      writeRpcAddress(out, senderAddress)
      writeRpcAddress(out, receiver.getHostPort)
      out.writeUTF(receiver.getName)
      out.writeObject(content)
    } finally {
      out.flush()
      out.close()
    }

    ByteBuffer.wrap(bos.toByteArray, 0, bos.toByteArray.length)
  }

  private def writeRpcAddress(out: ObjectOutputStream, rpcAddress: HostPort): Unit = {
    if (rpcAddress == null) {
      out.writeBoolean(false)
    } else {
      out.writeBoolean(true)
      out.writeUTF(rpcAddress.getHost)
      out.writeInt(rpcAddress.getPort)
    }
  }

  override def toString: String = s"RequestMessage($senderAddress, $receiver, $content)"
}

object RpcRequestMessage {
  private def readRpcAddress(in: ObjectInputStream): HostPort = {
    val hasRpcAddress = in.readBoolean()
    if (hasRpcAddress) {
      new HostPort(in.readUTF(), in.readInt())
    } else {
      null
    }
  }

  //实际上是 class RpcRequestMessage 的反序列化调用
  def apply(nettyEnv: NettyRpcEnv, bytes: ByteBuffer): RpcRequestMessage = {
    val bis = new ByteBufferInputStream(bytes)
    val in = new ObjectInputStream(bis)
    try {
      val senderAddress = readRpcAddress(in)
      val endpointAddress = new EndpointAddress(readRpcAddress(in), in.readUTF())
      val obj = in.readObject()
      new RpcRequestMessage(senderAddress, endpointAddress, obj)
    } finally {
      in.close()
    }
  }
}