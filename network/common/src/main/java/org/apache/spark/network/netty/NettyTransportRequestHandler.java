package org.apache.spark.network.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.util.NettyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyTransportRequestHandler extends TransportRequestHandler{

  private final Logger logger = LoggerFactory.getLogger(NettyTransportRequestHandler.class);
  /** The Netty channel that this handler is associated with. */
  private final Channel channel;
  
  public NettyTransportRequestHandler(Channel channel, NettyTransportClient reverseClient,
      RpcHandler rpcHandler) {
    super(NettyUtils.getRemoteAddress(channel),reverseClient, rpcHandler);
    this.channel = channel;
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * it will be logged and the channel closed.
   */
  public void respond(final Encodable result) {
    final String address = NettyUtils.getRemoteAddress(channel);
    channel.writeAndFlush(result).addListener(
      new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            logger.trace(String.format("Sent result %s to client %s", result, address));
          } else {
            logger.error(String.format("Error sending result %s to %s; closing connection",
              result, address), future.cause());
            channel.close();
          }
        }
      }
    );
  }

}
