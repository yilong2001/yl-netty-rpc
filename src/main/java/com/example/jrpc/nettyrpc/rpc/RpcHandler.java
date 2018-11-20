package com.example.jrpc.nettyrpc.rpc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;

/**
 * Created by yilong on 2018/3/13.
 */
public abstract class RpcHandler {
    private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();

    /**
     * Receive a single RPC message. Any exception thrown while in this method will be sent back to
     * the client in string form as a standard RPC failure.
     *
     * This method will not be called in parallel for a single TransportClient (i.e., channel).
     *
     * @param client A channel client which enables the handler to make requests back to the sender
     *               of this RPC. This will always be the exact same object for a particular channel.
     * @param message The serialized bytes of the RPC.
     * @param callback Callback which should be invoked exactly once upon success or failure of the
     *                 RPC.
     */
    public abstract void receive(
            HostPort client,
            ByteBuffer message,
            RpcResponseCallback callback);


    /**
     * Receives an RPC message that does not expect a reply. The default implementation will
     * call "{@link #receive(HostPort, ByteBuffer, RpcResponseCallback)}" and log a warning if
     * any of the callback methods are called.
     *
     * @param client A channel client which enables the handler to make requests back to the sender
     *               of this RPC. This will always be the exact same object for a particular channel.
     * @param message The serialized bytes of the RPC.
     */
    public void receive(HostPort client, ByteBuffer message) {
        receive(client, message, ONE_WAY_CALLBACK);
    }

    /**
     * Invoked when the channel associated with the given client is active.
     */
    public void channelActive(HostPort client) { }

    /**
     * Invoked when the channel associated with the given client is inactive.
     * No further requests will come from this client.
     */
    public void channelInactive(HostPort client) { }

    public void exceptionCaught(Throwable cause, HostPort client) { }

    private static class OneWayRpcCallback implements RpcResponseCallback {
        private static final Log logger = LogFactory.getLog(OneWayRpcCallback.class);

        @Override
        public void onSuccess(ByteBuffer response) {
            logger.warn("Response provided for one-way RPC.");
        }

        @Override
        public void onFailure(Throwable e) {
            logger.error("Error response provided for one-way RPC.", e);
        }

    }

}
