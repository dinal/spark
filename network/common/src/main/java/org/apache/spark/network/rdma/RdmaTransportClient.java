package org.apache.spark.network.rdma;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.StreamChunkId;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaTransportClient extends TransportClient {

  private final Logger logger = LoggerFactory.getLogger(RdmaTransportClient.class);

  private final String host;
  private final int port;
  private RdmaClientSession session;
  private URI uri;

  public RdmaTransportClient(String host, int port, RdmaClientContext ctx) {
    this.host = host;
    this.port = port;
    try {
      uri = new URI("rdma://" + host + ":"+port);
      session = new RdmaClientSession(uri, ctx);
    } catch (URISyntaxException e) {
      logger.error("Error creating RDMA uri");
    }
  }

  @Override
  public boolean isActive() {
    return session.isActive();
  }
  
  public boolean isWorking() {
    return session.isWorking();
  }

  @Override
  public void fetchChunk(long streamId, int chunkIndex, ChunkReceivedCallback callback) {
    logger.debug("Sending fetch chunk request {} to {}", chunkIndex, uri);
    final StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
    session.write(new RdmaMessage(new ChunkFetchRequest(streamChunkId)), streamChunkId, callback);

  }

  @Override
  public long sendRpc(ByteBuffer message, final RpcResponseCallback callback) {
    logger.debug("Sending RPC to {}:{}", host, port);
    final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
    session.write(new RdmaMessage(new RpcRequest(requestId, new NioManagedBuffer(message))), requestId, callback);
    return requestId;
  }

  @Override
  public void close() {
    session.close();
  }
  
  /**
   * return if reset was performed
   * @return if false the connection is closed
   */
  public boolean reset() {
    Boolean connected = session.isActive();
    if (connected)
      session.reset();
    return connected;
  }
  
  public SocketAddress getSocketAddress() {
    return new InetSocketAddress(host, port);
  }

  //TODO FIX RpcIntegrationSuite that uses this
  @VisibleForTesting
  public TransportResponseHandler getHandler() {
    return null;
  }

  //Methods relevant only for RPC not shuffle//
  
  public void stream(final String streamId, final StreamCallback callback) {
    throw new UnsupportedOperationException();
  }
  
  public void send(ByteBuffer message) {
    throw new UnsupportedOperationException();
  }
}
