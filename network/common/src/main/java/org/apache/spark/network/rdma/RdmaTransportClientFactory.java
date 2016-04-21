package org.apache.spark.network.rdma;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.accelio.jxio.ClientSession;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaTransportClientFactory implements TransportClientFactory {

  private final Logger logger = LoggerFactory.getLogger(RdmaTransportClientFactory.class);
  private HashMap<String, List<RdmaTransportClient>> connectionPool =
      new HashMap<String, List<RdmaTransportClient>>();

  public RdmaTransportClientFactory(TransportConf conf) {
    RdmaClientContext.createContexts(conf.clientThreads());
  }

  @Override
  public void close() {
    RdmaClientContext.requestToClose();
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
    synchronized (connectionPool) {
      List<RdmaTransportClient> clientPool = connectionPool.get(remoteHost);
      if (clientPool == null) {
        clientPool = new LinkedList<RdmaTransportClient>();
      }
      for (RdmaTransportClient client : clientPool) {
        if (client.isActive() && !client.isWorking()) {
          client.reset();
          return client;
        }
      }
      RdmaTransportClient newClient = null;
      while (newClient == null) {
        newClient = new RdmaTransportClient(remoteHost, remotePort);
        if (newClient.isActive()) {
          clientPool.add(newClient);
          connectionPool.put(remoteHost, clientPool);
        }
      }
      return newClient;
    }
  }
}
