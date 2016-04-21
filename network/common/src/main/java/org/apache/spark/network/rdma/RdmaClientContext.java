package org.apache.spark.network.rdma;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.accelio.jxio.ClientSession;
import org.accelio.jxio.EventQueueHandler;
import org.accelio.jxio.Msg;
import org.accelio.jxio.MsgPool;
import org.accelio.jxio.jxioConnection.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaClientContext implements Runnable, MessageProvider {

  private static LinkedList<RdmaClientContext> contexts = new LinkedList<RdmaClientContext>();
  private static boolean CLOSE = false;
  private static volatile Random rand = new Random();
  private final Logger logger = LoggerFactory.getLogger(RdmaClientContext.class);
  private static int CLIENT_BUF_COUNT = 100;
  private final int REQUEST_BATCH = 3;
  private final EventQueueHandler eqh;
  private final MsgPool msgPool;
  private final Thread contextThread;
  private ConcurrentLinkedQueue<Task> tasks = new ConcurrentLinkedQueue<Task>();
  private ConcurrentLinkedQueue<RdmaClientSession> sessions = new ConcurrentLinkedQueue<RdmaClientSession>();
  private int msgsSent;

  public static void createContexts(int numThreads) {
    for (int i=0; i< numThreads; i++) {
      contexts.add(new RdmaClientContext());
    }
  }
  protected static RdmaClientContext getContext(RdmaClientSession ses) {
    RdmaClientContext ctx = contexts.get(rand.nextInt(contexts.size()));
    ctx.sessions.add(ses);
    return ctx;
  }
 
  public RdmaClientContext() {
    logger.debug("starting Context");
    eqh = new EventQueueHandler(null);
    msgPool = new MsgPool(CLIENT_BUF_COUNT, Constants.MSGPOOL_BUF_SIZE, Constants.MSGPOOL_BUF_SIZE);
    contextThread = new Thread(this);
    contextThread.start();
  }

  public void run() {
    while (!CLOSE) {
      sendRequests();
      int ret = eqh.runEventLoop(EventQueueHandler.INFINITE_EVENTS, 10000);
      if (ret == -1) {
        logger.error(this + " runEventLoop returned with error: "+eqh.getCaughtException());
      }
    }
    logger.debug(this+" closing client context, sent "+msgsSent);
    eqh.stop();
    eqh.close();
    msgPool.deleteMsgPool();
  }

  private void sendRequests() {
    while (!tasks.isEmpty()) {
      Task entry = null;
      entry = tasks.peek();
      try {
        //send empty
        if (entry.req == null) {
          Msg m = getMsg();
          if (m == null) {
            //no msgs in pool
            break;
          } else {
            logger.trace(this+" sending empty msg, entry="+entry.req+" to: "+entry.ses);
            entry.ses.sendRequest(m);
            msgsSent++;
            tasks.poll();
          }
         //send regular req 
        } else {
          List<Msg> msgsToSend = entry.req.encode(this);
          if (msgsToSend.isEmpty()) {
            break;
          }
          for (Msg m : msgsToSend) {
            msgsSent++;
            logger.trace(this+" sending req entry="+entry.req+" msg="+m+" to: "+entry.ses);
            entry.ses.sendRequest(m);
          }
          if (entry.req.encodedFully) {
            tasks.poll();
          }
        }
      } catch (Exception e) {
        tasks.poll();
        String errorMsg = String.format("Failed to send RPC %s from %s: %s", entry.req.msg,
            entry.ses, e.getMessage());
        logger.error(errorMsg, e.getMessage());
      }
    }
  }

  // msg can be null
  public Msg getMsg() {
    if (msgPool.count() == 0) {
      return null;
    }
    Msg msg = msgPool.getMsg();
    return msg;
  }

  public EventQueueHandler getEqh() {
    return eqh;
  }
  
  public void retunrMsg(Msg msg) {
    msgPool.releaseMsg(msg);
  }

  public void addTask(RdmaMessage req, ClientSession ses) {
    synchronized (tasks) {
     // int before = tasks.size();
      tasks.add(new Task(req,ses));
    //  logger.info("added to task, before="+before+" after="+tasks.size());
    }
    if (tasks.size() > REQUEST_BATCH) {
      eqh.breakEventLoop();
    }
  }

  public void updateClosed(RdmaClientSession rdmaClientSession) {
   sessions.remove(rdmaClientSession);
   if (sessions.size() == 0 && CLOSE) {
     eqh.breakEventLoop();
   }
  }
  public static void requestToClose() {
   CLOSE = true;
  }
  
  class Task {
    public RdmaMessage req;
    public ClientSession ses;
    public Task(RdmaMessage req, ClientSession ses) {
      this.req = req;
      this.ses = ses;
    }
  }
}
