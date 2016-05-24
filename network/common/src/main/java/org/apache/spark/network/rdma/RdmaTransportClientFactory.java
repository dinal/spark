package org.apache.spark.network.rdma;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
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
  private ConcurrentHashMap<InetSocketAddress, ClientPool> connectionPool =
      new ConcurrentHashMap<InetSocketAddress, ClientPool>();
  private int numClientsCreated;
  private int numClientsReused;
  private Random rand = new Random();
  private LinkedList<RdmaClientContext> contexts = new LinkedList<RdmaClientContext>();
  private int ctxIndex = 0;
  //private TimerStats stats;

  private class ClientPool {
    List<RdmaTransportClient> clients;
    Object lock;

    public ClientPool() {
      clients = new LinkedList<RdmaTransportClient>();
      lock = new Object();
    }
  }
  
  public RdmaTransportClientFactory(TransportConf conf) {
    createContexts(Math.max(conf.clientThreads(), 1));
   // stats = new TimerStats(2000, 0);
  }

  @Override
  public void close() {
    logger.info("Closing RdmaClientFactory, num client creted "+ numClientsCreated+", num clients reused "+numClientsReused);
    for (RdmaClientContext ctx : contexts) {
      ctx.requestToClose();
    }
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (RdmaTransportClient client : clientPool.clients) {
        if (client != null) {
          JavaUtils.closeQuietly(client);
        }
      }
    }
   // stats.printRecords();
    connectionPool.clear();
  }

  public TransportClient createUnmanagedClient(String remoteHost, int remotePort) throws IOException {
    return createClient(remoteHost, remotePort);
  }

  @Override
  public TransportClient createClient(String remoteHost, int remotePort) throws IOException {
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
    long timeBefore = System.nanoTime();
    long timeAfter;
    ClientPool clientPool = connectionPool.get(address);
    if (clientPool == null) {
      connectionPool.putIfAbsent(address, new ClientPool());
      clientPool = connectionPool.get(address);
    }

    synchronized (clientPool.lock) {
      for (RdmaTransportClient client : clientPool.clients) {
        if (client.isActive() && !client.isWorking()) {
          client.reset();
          numClientsReused++;
          timeAfter = System.nanoTime();
          logger.debug("Reuse Client:"+ (timeAfter- timeBefore));
         // stats.addRecord("Reuse Client", (timeAfter- timeBefore));
          return client;
        }
      }
    //}
      RdmaTransportClient newClient = null;
      while (newClient == null) {
        newClient = new RdmaTransportClient(remoteHost, remotePort, getContext());
        if (newClient.isActive()) {
          numClientsCreated++;
         // synchronized (clientPool.lock) {
            clientPool.clients.add(newClient);
         // }
          //connectionPool.put(remoteHost, clientPool);
        } else {
          logger.warn("Client not active "+newClient);
          newClient = null;
        }
      }
      
      timeAfter = System.nanoTime();
      logger.debug("New Client:"+ (timeAfter- timeBefore));
      //stats.addRecord("New Client", (timeAfter- timeBefore));
      return newClient;
    }
  }
  

  private void createContexts(int numThreads) {
    for (int i=0; i< numThreads; i++) {
      contexts.add(new RdmaClientContext());
    }
  }

  private RdmaClientContext getContext() {
    RdmaClientContext ctx = contexts.get(ctxIndex);
    ctxIndex ++;
    ctxIndex = ctxIndex % contexts.size();
    logger.debug("New ctx :"+ctxIndex);
    return ctx;
  }

}
