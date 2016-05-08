package org.apache.spark.network.rdma;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.Random;

import org.accelio.jxio.EventName;
import org.accelio.jxio.EventQueueHandler;
import org.accelio.jxio.EventReason;
import org.accelio.jxio.ServerPortal;
import org.accelio.jxio.ServerSession;
import org.accelio.jxio.ServerSession.SessionKey;
import org.accelio.jxio.WorkerCache.Worker;
import org.accelio.jxio.WorkerCache.WorkerProvider;
import org.apache.spark.network.server.TransportServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaTransportServer implements Runnable, TransportServer, WorkerProvider {
  private final Logger logger = LoggerFactory.getLogger(RdmaTransportServer.class);
  private final int NUM_WORKERS;
  private Thread mListenerThread;
  private ServerPortal mListener;
  private EventQueueHandler mEqh;
  private Random rand = new Random();
  private RdmaTransportContext context;

  private boolean stop = false;
  private int newSession = 0;
  private LinkedList<RdmaServerWorker> serverWorkers = new LinkedList<RdmaServerWorker>();

  public RdmaTransportServer(RdmaTransportContext context, InetSocketAddress address) {
    NUM_WORKERS = context.getConf().serverThreads();
    logger.info("New RdmaTransportServer listening on " + address+" starting "+NUM_WORKERS+" workers");
    try {
      URI uri = new URI("rdma://" + address.getHostName() + ":" + address.getPort());
      mEqh = new EventQueueHandler(null);
      mListener = new ServerPortal(mEqh, uri, new PortalServerCallbacks(), this);
      for (int i = 1; i <= NUM_WORKERS; i++) {
        serverWorkers.add(new RdmaServerWorker(mListener.getUriForServer()));
      }
      this.context = context;
      mListenerThread = new Thread(this);
      mListenerThread.start();
    } catch (URISyntaxException e) {
      logger.error("Can't connect to RDMA server " + e);
    }
  }

  public class PortalServerCallbacks implements ServerPortal.Callbacks {

    public void onSessionEvent(EventName session_event, EventReason reason) {
      logger.debug("got event " + session_event.toString() + "because of " + reason.toString());
    }

    public void onSessionNew(SessionKey sesKey, String srcIP, Worker workerHint) {
      newSession++;
      RdmaServerWorker w;
      if (workerHint == null) {
        w = getWorker();
      } else {
        w = (RdmaServerWorker)workerHint;
      }
      logger.debug("onSessionNew "+srcIP+" forwarding to: "+w);
      RdmaSessionProcesser processer = new RdmaSessionProcesser(
          w, sesKey.getUri(), context.getRpcHandler());
      ServerSession session = new ServerSession(sesKey, processer.getSessionCallbacks());
      processer.setSession(session);
      w.addNewSession(session);
      mListener.forward(w.getPortal(), session);
    }
  }

  @Override
  public void run() {
    for (RdmaServerWorker worker : serverWorkers) {
      worker.start();
    }
    int ret = mEqh.runEventLoop(EventQueueHandler.INFINITE_EVENTS, EventQueueHandler.INFINITE_DURATION);
    if (ret == -1) {
      logger.error(this + " runEventLoop returned with error: "+mEqh.getCaughtException());
    }
    //closing
    logger.debug(this+ " Number of session: "+newSession);
    for (RdmaServerWorker worker : serverWorkers) {
      worker.close();
    }
    mEqh.stop();
    mEqh.close();
  }

  @Override
  public void close() {
   logger.info("close() server");
   stop = true;
   mEqh.breakEventLoop();
  }

  public int getPort() {
    return mListener.getUri().getPort();
  }

  @Override
  public RdmaServerWorker getWorker() {
    return serverWorkers.get(rand.nextInt(NUM_WORKERS));
  }
}