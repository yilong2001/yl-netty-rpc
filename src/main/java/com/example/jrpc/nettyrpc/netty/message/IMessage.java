package com.example.jrpc.nettyrpc.netty.message;

import com.example.jrpc.nettyrpc.netty.buffer.ManagedBuffer;
import com.example.jrpc.nettyrpc.netty.message.req.OneWayMessage;
import com.example.jrpc.nettyrpc.netty.message.req.RpcRequest;
import com.example.jrpc.nettyrpc.netty.message.rsp.RpcFailure;
import com.example.jrpc.nettyrpc.netty.message.rsp.RpcResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by yilong on 2018/3/7.
 */
public abstract class IMessage {
    private static final Logger logger = LoggerFactory.getLogger(MessageDecoder.class);

    abstract public IMessage.MessageType type();

    //对于普通的ByteBuf，数据头与数据本身都在byte[]保存，isBodyInFrame值为true
    //对于File/Stream类型的数据，一个Frame只有数据头，
    // ,而数据体则从File/Stream的缓冲/原始内存中逐个读取，并直接写入目标channel
    // ,目的是减少数据拷贝，即 zero-copy
    // ,此时isBodyInFrame值为false
    private final ManagedBuffer body;
    private final boolean isBodyInFrame;

    protected IMessage(ManagedBuffer body, boolean isBodyInFrame) {
        this.body = body;
        this.isBodyInFrame = isBodyInFrame;
    }

    public ManagedBuffer body() { return this.body; }
    public boolean isBodyInFrame() { return this.isBodyInFrame; }

    public enum MessageType {
        RpcRequest(1),RpcResponse(2),RpcFailure(3),OneWayMessage(5);
        private final byte id;

        MessageType(int id) { this.id = (byte)id;}
        public byte id() { return id; }
        public int encodedLength() { return 1; }
        public void encode(ByteBuf buf) { buf.writeByte(id); }
    }

    public int encodedLengthOnFrameHeader() {return 8 + 1; }
    abstract public int encodedLength();

    //在channel中传输的数据，其编码格式为：[length], [type], [data]
    // 其中 length = 8 bytes + type length(1 byte) + data length
    abstract public void encode(ByteBuf buf);

    //
    public static void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        byte id = -1;
        IMessage decoded = null;
        try {
            id = in.readByte();
            decoded = decode(id, in);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        assert decoded.type().id() == id;
        out.add(decoded);
    }

    private static IMessage decode(int id, ByteBuf in) {
        switch (id) {
            case 0:
                //reserved
                throw new IllegalArgumentException("Unexpected message id : " + id);
            case 1:
                return RpcRequest.decode(in);

            case 2:
                return RpcResponse.decode(in);

            case 3:
                return RpcFailure.decode(in);

            case 5:
                return OneWayMessage.decode(in);

            default:
                throw new IllegalArgumentException("Unexpected message id : " + id);
        }
    }
}
