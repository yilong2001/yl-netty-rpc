package com.example.jrpc.nettyrpc.netty;

import com.example.jrpc.nettyrpc.netty.handler.server.INettyServerBootstrapWrapper;
import com.example.jrpc.nettyrpc.rpc.RpcConfig;
import com.example.jrpc.nettyrpc.rpc.RpcHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by yilong on 2018/3/14.
 */
public class NettyServer implements Closeable {
    private static final Log logger = LogFactory.getLog(NettyServer.class);

    private final NettyContext nettyContext;
    private final String host;
    private final int port;
    private final RpcHandler rpcHandler;
    private final List<INettyServerBootstrapWrapper> bootstraps;
    private final String threadPoolPrefix;
    private final RpcConfig rpcConfig;

    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;

    public NettyServer(RpcConfig config, NettyContext nettyContext, String host, int port, RpcHandler rpcHandler, List<INettyServerBootstrapWrapper> bootstraps) {
        this.rpcConfig = config;
        this.nettyContext = nettyContext;
        this.host = host;
        this.port = port;
        this.rpcHandler = rpcHandler;
        this.bootstraps = bootstraps;
        this.threadPoolPrefix = config.getModuleName();

        try {
            init(this.host, this.port);
        } catch (RuntimeException e) {
            try {
                this.close();
            } catch (Exception e1) {
                logger.error("IOException should not have been thrown.", e1);
            }
            throw e;
        }
    }

    public int getPort() {
        if (port == -1) {
            throw new IllegalStateException("Server not initialized");
        }
        return port;
    }

    private void init(String hostToBind, int portToBind) {
        ThreadFactory threadFactory = new DefaultThreadFactory(threadPoolPrefix, true);
        EventLoopGroup bossGroup = new NioEventLoopGroup(rpcConfig.getServerThreads(), threadFactory);
        EventLoopGroup workerGroup = bossGroup;

        PooledByteBufAllocator allocator = NettyUtils.createPooledByteBufAllocator(
                true, true /* allowCache */, rpcConfig.getServerThreads());

        bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, allocator)
                .childOption(ChannelOption.ALLOCATOR, allocator);

        if (rpcConfig.getBackLog() > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, rpcConfig.getBackLog());
        }

        if (rpcConfig.getReceiveBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_RCVBUF, rpcConfig.getReceiveBuf());
        }

        if (rpcConfig.getSendBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, rpcConfig.getSendBuf());
        }

        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                RpcHandler handler = rpcHandler;
                for (INettyServerBootstrapWrapper bootstrap : bootstraps) {
                    handler = bootstrap.doBootstrap(ch, handler);
                }
                nettyContext.initializePipeline(ch, handler);
            }
        });

        InetSocketAddress address = new InetSocketAddress(hostToBind, portToBind);
        channelFuture = bootstrap.bind(address);
        channelFuture.syncUninterruptibly();

        logger.debug(String.format("Shuffle server started on port: {%d}", port));
    }

    @Override
    public void close() {
        if (channelFuture != null) {
            // close is a local operation and should finish within milliseconds; timeout just to be safe
            channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
            channelFuture = null;
        }
        if (bootstrap != null && bootstrap.group() != null) {
            bootstrap.group().shutdownGracefully();
        }
        if (bootstrap != null && bootstrap.childGroup() != null) {
            bootstrap.childGroup().shutdownGracefully();
        }
        bootstrap = null;
    }


}
