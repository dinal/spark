package org.apache.spark.network.rdma;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.StreamChunkId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaTransportClient extends TransportClient {

  private final Logger logger = LoggerFactory.getLogger(RdmaTransportClient.class);

  private String host;
  private int port;
  private RdmaClientContext context;
  private URI uri;;

  public RdmaTransportClient(String host, int port) {
    try {
      this.host = host;
      this.port = port;
      uri = new URI("rdma://" + host + ":"+port);
      context = new RdmaClientContext(uri);
    } catch (URISyntaxException e) {
      logger.error("Error creating RDMA uri");
      return;
    }
  }

  @Override
  public boolean isActive() {
    return context.isActive();
  }

  @Override
  public void fetchChunk(long streamId, int chunkIndex, ChunkReceivedCallback callback) {
    logger.debug("Sending fetch chunk request {} to {}", chunkIndex, uri);
    final StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
    context.write(
        new RdmaMessage(new ChunkFetchRequest(streamChunkId), (long) streamChunkId.hashCode()),
        callback);

  }

  @Override
  public void sendRpc(byte[] message, RpcResponseCallback callback) {
    logger.trace("Sending RPC to {}:{}", host, port);
    final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
    context.write(new RdmaMessage(new RpcRequest(requestId, message), requestId), callback);
  }

  @Override
  public void close() {
    context.close();
  }
  
  /**
   * return if reset was performed
   * @return if false the connection is closed
   */
  public boolean reset() {
    Boolean connected = context.isConnected();
    if (connected)
      context.reset();
    return connected;
  }
}
