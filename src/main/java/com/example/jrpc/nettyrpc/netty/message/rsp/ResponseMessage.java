package com.example.jrpc.nettyrpc.netty.message.rsp;

import com.example.jrpc.nettyrpc.netty.buffer.ManagedBuffer;
import com.example.jrpc.nettyrpc.netty.message.IMessage;

/**
 * Created by yilong on 2018/3/11.
 */
public abstract class ResponseMessage extends IMessage {
    protected ResponseMessage(ManagedBuffer body, boolean isBodyInFrame) {
        super(body, isBodyInFrame);
    }
}
