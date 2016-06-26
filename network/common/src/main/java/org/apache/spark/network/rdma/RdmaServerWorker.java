package org.apache.spark.network.rdma;

import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.accelio.jxio.EventName;
import org.accelio.jxio.EventQueueHandler;
import org.accelio.jxio.EventReason;
import org.accelio.jxio.MsgPool;
import org.accelio.jxio.ServerPortal;
import org.accelio.jxio.ServerSession;
import org.accelio.jxio.ServerSession.SessionKey;
import org.accelio.jxio.WorkerCache.Worker;
import org.accelio.jxio.jxioConnection.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RdmaServerWorker extends Thread implements Worker {
  
  private final Logger logger = LoggerFactory.getLogger(RdmaServerWorker.class);
  public static final int SERVER_BUFFER_SIZE = Constants.MSGPOOL_BUF_SIZE;
  private static final int SERVER_INC_BUFFER = 500;
 // private static final int MAX_SESSIONS = 2;
  
  private final ServerPortal sp;
  private final EventQueueHandler mEqh;
  private ArrayList<MsgPool> mMsgPools = new ArrayList<MsgPool>();;;
  private boolean stop = false;
  private ConcurrentLinkedQueue<ServerSession> sessions = new ConcurrentLinkedQueue<ServerSession>();
 // private TimerStats stats;
  private int numHandledSessions = 0;
  
  
  public RdmaServerWorker(URI uri, int bufCount) {
    MsgPool pool = new MsgPool(bufCount, SERVER_BUFFER_SIZE, SERVER_BUFFER_SIZE);
    mMsgPools.add(pool);
    mEqh = new EventQueueHandler(new EqhCallbacks(SERVER_INC_BUFFER, SERVER_BUFFER_SIZE, SERVER_BUFFER_SIZE));
    mEqh.bindMsgPool(pool);
    sp = new ServerPortal(mEqh, uri, new PortalServerCallbacks());
   // stats = new TimerStats(100, 0);
  }
  
  public void run() {
    int ret = mEqh.runEventLoop(EventQueueHandler.INFINITE_EVENTS, EventQueueHandler.INFINITE_DURATION);
    if (ret == -1) {
      logger.error(this + " runEventLoop returned with error: "+mEqh.getCaughtException());
    }
    mEqh.stop();
    mEqh.close();
    for (MsgPool mp : mMsgPools) {
      mEqh.releaseMsgPool(mp);
      mp.deleteMsgPool();
    }
  }

  @Override
  public boolean isFree() {
   return true;//sessions.size() < MAX_SESSIONS;
  }

  public void close() {
    stop = true;
    logger.debug(this.toString() + " closing worker, session "+sessions.size());
    for (ServerSession ses : sessions) {
      if (!ses.getIsClosing()) {
        ses.close();
      }
    }
    checkClose();
  }

  public void addNewSession(ServerSession ses) {
    sessions.add(ses);
    numHandledSessions++;
  }

  public void updateClosed(ServerSession ses) {
    sessions.remove(ses);
    checkClose();
  }
  
  private void checkClose() {
    if (stop && sessions.isEmpty()) {
      //stats.addRecord("NumSessions", numHandledSessions);
      mEqh.breakEventLoop();
    }
  }

  class EqhCallbacks implements EventQueueHandler.Callbacks {
    private final RdmaServerWorker mOuter = RdmaServerWorker.this;
    private final int mNumMsgs;
    private final int mInMsgSize;
    private final int mOutMsgSize;

    public EqhCallbacks(int msgs, int in, int out) {
      mNumMsgs = msgs;
      mInMsgSize = in;
      mOutMsgSize = out;
    }

    public MsgPool getAdditionalMsgPool(int in, int out) {
      logger.info(this+" adding additional msg pool");
      MsgPool mp = new MsgPool(mNumMsgs, mInMsgSize, mOutMsgSize);
      mOuter.mMsgPools.add(mp);
      return mp;
    }
  }
  
  private final class PortalServerCallbacks implements ServerPortal.Callbacks {

    public void onSessionEvent(EventName event, EventReason reason) {
      logger.debug(RdmaServerWorker.this.toString() + " GOT EVENT " + event.toString() + "because of "
              + reason.toString());
     // TimerStats.addRecord("Server new connection", 1);
    }

    public void onSessionNew(SessionKey sesKey, String srcIP, Worker workerHint) {}
  }
  
  public ServerPortal getPortal() {
    return sp;
  }
}
