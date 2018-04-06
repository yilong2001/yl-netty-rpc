package com.example.srpc.nettyrpc.serde

import java.io._
import java.nio.ByteBuffer

/**
  * Created by yilong on 2018/4/6.
  */
class RpcSerializer {
  def serialize[T](t: T): ByteBuffer = {
    val bos = new ByteArrayOutputStream
    val oout = new ObjectOutputStream(bos)
    oout.writeObject(t)
    oout.flush()
    oout.close()
    ByteBuffer.wrap(bos.toByteArray, 0, bos.toByteArray.length)
  }

  def deserialize1[T](bytes: ByteBuffer): T = {
    val bis = new ByteArrayInputStream(bytes.array)
    val in = new ObjectInputStream(bis)
    val x1 = in.readUTF
    val t = in.readObject.asInstanceOf[T]
    in.close()
    t
  }

  def deserialize[T](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = new ObjectInputStream(bis)
    var obj : T = null.asInstanceOf[T]
    try
      obj = in.readObject.asInstanceOf[T]
    finally in.close()
    obj
  }
}
