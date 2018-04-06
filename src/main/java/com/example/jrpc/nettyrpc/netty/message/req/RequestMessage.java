package com.example.jrpc.nettyrpc.netty.message.req;

import com.example.jrpc.nettyrpc.netty.buffer.ManagedBuffer;
import com.example.jrpc.nettyrpc.netty.message.IMessage;

/**
 * Created by yilong on 2018/3/11.
 *
 * RequestMessage format on channel:
 * [is has sender address, [sender address]], [endpoint address], [Request]
 *
 * Request format on channel:
 * [Request Id], [Content]
 */
public abstract class RequestMessage extends IMessage {
    protected RequestMessage(ManagedBuffer body, boolean isBodyInFrame) {
        super(body, isBodyInFrame);
    }
}
