package com.example.jrpc.nettyrpc.netty;

import com.example.jrpc.nettyrpc.netty.handler.NettyChannelCommonHandler;
import com.example.jrpc.nettyrpc.netty.handler.client.ConcurrentNettyChannelClient;
import com.example.jrpc.nettyrpc.netty.handler.client.INettyClientBootstrapWrapper;
import com.example.jrpc.nettyrpc.rpc.HostPort;
import com.example.jrpc.nettyrpc.rpc.RpcConfig;
import com.google.common.base.Throwables;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by yilong on 2018/3/14.
 */
public class NettyClientFactory implements Closeable {
    /** A simple data structure to track the pool of clients between two peer nodes. */
    private static class ClientPool {
        ConcurrentNettyChannelClient[] clients;
        Object[] locks;

        ClientPool(int size) {
            clients = new ConcurrentNettyChannelClient[size];
            locks = new Object[size];
            for (int i = 0; i < size; i++) {
                locks[i] = new Object();
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(NettyClientFactory.class);

    private final NettyContext context;
    private final RpcConfig conf;
    private final List<INettyClientBootstrapWrapper> clientBootstraps;
    private final ConcurrentHashMap<HostPort, ClientPool> connectionPool;

    private final int clientNumPerPeer = 10;

    private final Random random = new Random();

    private EventLoopGroup workerGroup;
    private PooledByteBufAllocator pooledAllocator;

    public NettyClientFactory(RpcConfig config, NettyContext context,
                              List<INettyClientBootstrapWrapper> bootstrapWrappers) {
        this.context = context;
        this.clientBootstraps = bootstrapWrappers;
        this.conf = config;
        this.connectionPool = new ConcurrentHashMap<HostPort, ClientPool>();

        ThreadFactory threadFactory = new DefaultThreadFactory(conf.getModuleName()+"_client", true);
        this.workerGroup = new NioEventLoopGroup(conf.getServerThreads(), threadFactory);

        this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
                true, false /* allowCache */, conf.getClientThreads());
    }

    public ConcurrentNettyChannelClient createClient(String remoteHost, int remotePort)
            throws IOException, InterruptedException {
        // Get connection from the connection pool first.
        // If it is not found or not active, create a new one.
        HostPort remoteHostPort = new HostPort(remoteHost, remotePort);

        // Create the ClientPool if we don't have it yet.
        ClientPool clientPool = connectionPool.get(remoteHostPort);
        if (clientPool == null) {
            connectionPool.putIfAbsent(remoteHostPort, new ClientPool(clientNumPerPeer));
            clientPool = connectionPool.get(remoteHostPort);
        }

        int clientIndex = random.nextInt(clientNumPerPeer);
        ConcurrentNettyChannelClient cachedClient = clientPool.clients[clientIndex];

        if (cachedClient != null && cachedClient.isActive()) {
            // Make sure that the channel will not timeout by updating the last use time of the
            // handler. Then check that the client is still alive, in case it timed out before
            // this code was able to update things.
            NettyChannelCommonHandler handler = cachedClient.getChannel().pipeline()
                    .get(NettyChannelCommonHandler.class);
            synchronized (handler) {
                handler.getResponseHandler().updateTimeOfLastRequest();
            }

            if (cachedClient.isActive()) {
                logger.trace("Returning cached connection to {}: {}",
                        cachedClient.getSocketAddress(), cachedClient);
                return cachedClient;
            }
        }

        // If we reach here, we don't have an existing connection open. Let's create a new one.
        // Multiple threads might race here to create new connections. Keep only one of them active.
        final long preResolveHost = System.nanoTime();
        final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
        final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
        if (hostResolveTimeMs > 2000) {
            logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
        } else {
            logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
        }

        synchronized (clientPool.locks[clientIndex]) {
            cachedClient = clientPool.clients[clientIndex];

            if (cachedClient != null) {
                if (cachedClient.isActive()) {
                    logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
                    return cachedClient;
                } else {
                    logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
                }
            }
            clientPool.clients[clientIndex] = createClient(remoteHostPort);
            return clientPool.clients[clientIndex];
        }
    }

    private ConcurrentNettyChannelClient createClient(HostPort hostPort)
            throws IOException, InterruptedException {
        logger.debug("Creating new connection to {}", hostPort);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                // Disable Nagle's Algorithm since we don't want packets to wait
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.getConnectionTimeoutMs())
                .option(ChannelOption.ALLOCATOR, pooledAllocator);

        final AtomicReference<ConcurrentNettyChannelClient> clientRef = new AtomicReference<>();
        final AtomicReference<Channel> channelRef = new AtomicReference<>();

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                NettyChannelCommonHandler clientHandler = context.initializePipeline(ch);
                clientRef.set(clientHandler.getClient());
                channelRef.set(ch);
            }
        });

        // Connect to the remote server
        long preConnect = System.nanoTime();
        ChannelFuture cf = bootstrap.connect(new InetSocketAddress(hostPort.getHost(), hostPort.getPort()));
        if (!cf.await(conf.getConnectionTimeoutMs())) {
            throw new IOException(
                    String.format("Connecting to %s timed out (%s ms)",
                            hostPort,
                            conf.getConnectionTimeoutMs()));
        } else if (cf.cause() != null) {
            throw new IOException(String.format("Failed to connect to %s", hostPort), cf.cause());
        }

        ConcurrentNettyChannelClient client = clientRef.get();
        Channel channel = channelRef.get();
        assert client != null : "Channel future completed successfully with null client";

        // Execute any client bootstraps synchronously before marking the Client as successful.
        long preBootstrap = System.nanoTime();
        logger.debug("Connection to {} successful, running bootstraps...", hostPort);
        try {
            for (INettyClientBootstrapWrapper clientBootstrap : clientBootstraps) {
                clientBootstrap.doBootstrap(client, channel);
            }
        } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
            long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
            logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
            client.close();
            throw Throwables.propagate(e);
        }
        long postBootstrap = System.nanoTime();

        logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
                hostPort,
                (postBootstrap - preConnect) / 1000000,
                (postBootstrap - preBootstrap) / 1000000);

        return client;
    }

    @Override
    public void close() throws IOException {
        for (ClientPool pool : connectionPool.values()) {
            for (ConcurrentNettyChannelClient client : pool.clients) {
                if (client != null) {
                    try {
                        client.close();
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }

            pool.clients = null;
        }

        connectionPool.clear();

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
    }
}
