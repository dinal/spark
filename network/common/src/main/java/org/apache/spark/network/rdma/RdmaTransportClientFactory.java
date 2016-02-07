package org.apache.spark.network.rdma;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.util.JavaUtils;

public class RdmaTransportClientFactory implements TransportClientFactory {

  private ConcurrentHashMap<String, List<RdmaTransportClient>> connectionPool =
      new ConcurrentHashMap<String, List<RdmaTransportClient>>();

  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (List<RdmaTransportClient> clientPool : connectionPool.values()) {
      for (RdmaTransportClient client : clientPool) {
        if (client != null) {
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();
  }

  public TransportClient createUnmanagedClient(String remoteHost, int remotePort) throws IOException {
    return createClient(remoteHost, remotePort);
  }

  @Override
  public TransportClient createClient(String remoteHost, int remotePort) throws IOException {
    List<RdmaTransportClient> clientPool = connectionPool.get(remoteHost);
    if (clientPool == null) {
      connectionPool.putIfAbsent(remoteHost, new LinkedList<RdmaTransportClient>());
      clientPool = connectionPool.get(remoteHost);
    }
    synchronized (clientPool) {
      for (RdmaTransportClient client : clientPool) {
        if (!client.isActive()) {
          if (client.reset()) {
            return client;
          } else {
            //remove from list since it's already closed
            clientPool.remove(client);
          }
        }
      }
      RdmaTransportClient newClient = new RdmaTransportClient(remoteHost, remotePort);
      clientPool.add(newClient);
      connectionPool.replace(remoteHost, clientPool);
      return newClient;
    }
  }

}
