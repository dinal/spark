package org.apache.spark.network.rdma;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.accelio.jxio.EventName;
import org.accelio.jxio.EventQueueHandler;
import org.accelio.jxio.EventReason;
import org.accelio.jxio.Msg;
import org.accelio.jxio.MsgPool;
import org.accelio.jxio.ServerPortal;
import org.accelio.jxio.ServerSession;
import org.accelio.jxio.ServerSession.SessionKey;
import org.accelio.jxio.WorkerCache.Worker;
import org.accelio.jxio.exceptions.JxioGeneralException;
import org.accelio.jxio.exceptions.JxioSessionClosedException;
import org.accelio.jxio.jxioConnection.Constants;
import org.accelio.jxio.jxioConnection.JxioConnection;
import org.apache.spark.network.server.TransportServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaTransportServer implements Runnable, TransportServer, ServerResponder {
  private final Logger logger = LoggerFactory.getLogger(RdmaTransportServer.class);
  public static final int SERVER_BUFFER_SIZE = Constants.MSGPOOL_BUF_SIZE;
  private static final int SERVER_INITIAL_BUFFER = 500;
  private static final int SERVER_INC_BUFFER = 50;
  private Thread mListenerThread;
  private ServerPortal mListener;
  private EventQueueHandler mEqh;
  private ArrayList<MsgPool> mMsgPools = new ArrayList<MsgPool>();
  private ExecutorService executers[] = new ExecutorService[20];
  private int executorNextIndex = 0;
  private RdmaTransportContext context;
  private HashMap<ServerSession, List<Msg>> responses = new HashMap<ServerSession, List<Msg>>();
  private static final int BATCH_RESPONSE = 5;
  private boolean stop;

  public RdmaTransportServer(RdmaTransportContext context, InetSocketAddress address) {
    logger.info("New RdmaTransportServer listening on " + address);
    try {
      URI uri = new URI("rdma://" + address.getHostName() + ":" + address.getPort());
      MsgPool pool = new MsgPool(SERVER_INITIAL_BUFFER, SERVER_BUFFER_SIZE, SERVER_BUFFER_SIZE);
      mMsgPools.add(pool);
      mEqh = new EventQueueHandler(new EqhCallbacks(SERVER_INC_BUFFER, SERVER_BUFFER_SIZE, SERVER_BUFFER_SIZE));
      mEqh.bindMsgPool(pool);
      mListener = new ServerPortal(mEqh, uri, new PortalServerCallbacks(), null);
      for (int i = 0; i < executers.length; i++) {
        executers[i] = Executors.newSingleThreadExecutor();
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
      if (session_event == EventName.PORTAL_CLOSED) {
        mEqh.breakEventLoop();
      }
    }

    public void onSessionNew(SessionKey sesKey, String srcIP, Worker workerHint) {
      RdmaSessionProcesser processer = new RdmaSessionProcesser(RdmaTransportServer.this,
          executers[executorNextIndex], sesKey.getUri(), context.getRpcHandler());
      ServerSession session = new ServerSession(sesKey, processer.callbacks);
      logger.info("onSessionNew uri=" + sesKey.getUri() + " session=" + session);
      processer.setSession(session);
      mListener.accept(session);
      executorNextIndex += 1;
      if (executorNextIndex == executers.length) {
        executorNextIndex = 0;
      }
    }
  }

  @Override
  public void run() {
    while (!stop) {
      int ret = mEqh.runEventLoop(EventQueueHandler.INFINITE_EVENTS, 1000);
      if (ret == -1) {
        logger.error(this.toString() + " exception occurred in eventLoop:"
            + mEqh.getCaughtException());
      }
      synchronized (responses) {
        for (Entry<ServerSession, List<Msg>> entry : responses.entrySet()) {
          for (Msg m : entry.getValue()) {
            try {
              logger.info("send on session: "+entry.getKey()+" msg: "+m);
              entry.getKey().sendResponse(m);
              //logger.info(this+" sending on session="+entry.getKey()+" msg="+entry.getValue());
            } catch (Exception e) {
              logger.error("Error Sending response to " + entry.getKey());
            }
          }
        }
        responses.clear();
      }
    }
    for (int i = 0; i < executers.length; i++) {
      executers[i].shutdown();
    }
    logger.info("server closing 2");
    mEqh.stop();
    logger.info("server closing 3");
    mEqh.close();
    logger.info("server closing 4");
    for (MsgPool mp : mMsgPools) {
      mp.deleteMsgPool();
    }
    synchronized (mEqh) {
      mEqh.notifyAll();
    }
  }

  @Override
  public void close() {
    logger.info("closing server");
    stop = true;
    synchronized (mEqh) {
      mEqh.breakEventLoop();
      try {
        mEqh.wait();
      } catch (Exception e) {
        logger.error("could not wait for server closing " + e.getMessage());
      }
    }
    logger.info("Server stopped");
  }

  public int getPort() {
    return mListener.getUri().getPort();
  }

  class EqhCallbacks implements EventQueueHandler.Callbacks {
    private final RdmaTransportServer mOuter = RdmaTransportServer.this;
    private final int mNumMsgs;
    private final int mInMsgSize;
    private final int mOutMsgSize;

    public EqhCallbacks(int msgs, int in, int out) {
      mNumMsgs = msgs;
      mInMsgSize = in;
      mOutMsgSize = out;
    }

    public MsgPool getAdditionalMsgPool(int in, int out) {
      MsgPool mp = new MsgPool(mNumMsgs, mInMsgSize, mOutMsgSize);
      logger.info(this.toString() + " " + mOuter.toString() + ": new MsgPool: " + mp);
      mOuter.mMsgPools.add(mp);
      return mp;
    }
  }

  @Override
  public void respond(ServerSession session, Msg msg) {
    //logger.info(this+" adding to respond list session="+session+" msg="+msg);
    synchronized (responses) {
      List<Msg> list = responses.get(session);
      if (list == null) {
        list = new LinkedList<Msg>();
        list.add(msg);
      } else {
        list.add(msg);
      }
      responses.put(session, list);
    }

    if (responses.size() >= BATCH_RESPONSE) {
      mEqh.breakEventLoop();
    }
  }
}