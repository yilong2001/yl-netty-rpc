package com.example.jrpc.nettyrpc.netty;

import com.example.jrpc.nettyrpc.netty.handler.server.INettyServerBootstrapWrapper;
import com.example.jrpc.nettyrpc.netty.message.MessageDecoder;
import com.example.jrpc.nettyrpc.netty.message.MessageEncoder;
import com.example.jrpc.nettyrpc.netty.message.TransportFrameDecoder;
import com.example.jrpc.nettyrpc.rpc.RpcConfig;
import com.example.jrpc.nettyrpc.netty.handler.NettyChannelCommonHandler;
import com.example.jrpc.nettyrpc.rpc.RpcHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yilong on 2018/3/13.
 */
public class NettyContext {
    private static final Logger logger = LoggerFactory.getLogger(NettyContext.class);

    private final RpcConfig config;
    private final RpcHandler rpcHandler;
    
    public NettyContext(RpcConfig config, RpcHandler rpcHandler) {
        this.config = config;
        this.rpcHandler = rpcHandler;
    }

    /** Create a server which will attempt to bind to a specific port. */
    public NettyServer createServer(int port, List<INettyServerBootstrapWrapper> bootstraps) {
        return new NettyServer(config, this, null, port, rpcHandler, bootstraps);
    }

    /** Create a server which will attempt to bind to a specific host and port. */
    public NettyServer createServer(
            String host, int port, List<INettyServerBootstrapWrapper> bootstraps) {
        return new NettyServer(config, this, host, port, rpcHandler, bootstraps);
    }

    /** Creates a new server, binding to any available ephemeral port. */
    public NettyServer createServer(List<INettyServerBootstrapWrapper> bootstraps) {
        return createServer(0, bootstraps);
    }

    public NettyServer createServer() {
        return createServer(0, new ArrayList<INettyServerBootstrapWrapper>());
    }

    public NettyChannelCommonHandler initializePipeline(SocketChannel channel) {
        return initializePipeline(channel, rpcHandler);
    }

    /**
     * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
     * has a {@link com.example.jrpc.nettyrpc.netty.handler.NettyChannelCommonHandler} to handle request or
     * response messages.
     *
     * @param channel The channel to initialize.
     * @param channelRpcHandler The RPC handler to use for the channel.
     *
     * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
     * be used to communicate on this channel. The TransportClient is directly associated with a
     * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
     */
    public NettyChannelCommonHandler initializePipeline(
            SocketChannel channel,
            RpcHandler channelRpcHandler) {
        try {
            NettyChannelCommonHandler channelHandler = new NettyChannelCommonHandler(channel, config, rpcHandler);
            channel.pipeline()
                    .addLast("encoder", MessageEncoder.INSTANCE)
                    .addLast("frameDecoder",new TransportFrameDecoder())
                            /*new LengthFieldBasedFrameDecoder(100000000,
                                    0,4,
                                    0,4))*/
                    .addLast("decoder", MessageDecoder.INSTANCE)
                    .addLast("idleStateHandler",
                            new IdleStateHandler(0,
                                    0,
                                    config.getConnectionTimeoutMs() / 1000))
                    // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
                    // would require more logic to guarantee if this were not part of the same event loop.
                    .addLast("handler", channelHandler);
            return channelHandler;
        } catch (RuntimeException e) {
            logger.error("Error while initializing Netty pipeline", e);
            throw e;
        }
    }

}
