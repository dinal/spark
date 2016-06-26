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
  private static boolean CLOSE = false;
  private final Logger logger = LoggerFactory.getLogger(RdmaClientContext.class);
  private final int SEND_TH = 20;
  private final EventQueueHandler eqh;
  private final MsgPool msgPool;
  private final Thread contextThread;
  private ConcurrentLinkedQueue<Task> tasks = new ConcurrentLinkedQueue<Task>();
  private ConcurrentLinkedQueue<RdmaClientSession> sessions = new ConcurrentLinkedQueue<RdmaClientSession>();
  private int msgsSent;
  public volatile int inFlight = 0;
 
  public RdmaClientContext(int bufCount) {
    logger.info("starting Context");
    eqh = new EventQueueHandler(null);
    msgPool = new MsgPool(bufCount, Constants.MSGPOOL_BUF_SIZE, Constants.MSGPOOL_BUF_SIZE);
    contextThread = new Thread(this);
    contextThread.start();
  }

  public void run() {
    while (!CLOSE) {
      sendRequests(null);
      int ret = eqh.runEventLoop(EventQueueHandler.INFINITE_EVENTS, EventQueueHandler.INFINITE_DURATION);
      if (ret == -1) {
        logger.error(this + " runEventLoop returned with error: "+eqh.getCaughtException());
      }
    }
    logger.debug(this+" closing client context, sent "+msgsSent);
    eqh.stop();
    eqh.close();
    msgPool.deleteMsgPool();
  }

  private void sendRequests(Msg msg) {
    Msg prevMsgToUse = msg;
    while (!tasks.isEmpty()) {
      Task entry = null;
      entry = tasks.peek();
      try {
        //send empty
        if (entry.req == null) {
          Msg m;
          if (prevMsgToUse != null) {
            m = prevMsgToUse;
            prevMsgToUse = null;
          } else {
            m = getMsg();
            if (m == null) {
              //no msgs in pool
              break;
            }
          }
          //entry = tasks.poll();
          logger.debug(this+" sending empty msg, entry="+entry.req+" to: "+entry.ses+", count: "+entry.count);
          entry.ses.sendRequest(m);
          inFlight++;
          msgsSent++;
          entry.count--;
          if (entry.count == 0) {
            tasks.poll();
          }
          /*if (entry.count != 0) {
            tasks.add(entry);  
            }*/
         //send regular req
        } else {
          List<Msg> msgsToSend = entry.req.encode(this, prevMsgToUse);
          if (msgsToSend.isEmpty()) {
            break;
          }
          prevMsgToUse = null;
          for (Msg m : msgsToSend) {
            msgsSent++;
            logger.debug(this+" sending req entry="+entry.req+" msg="+m+" to: "+entry.ses);
            entry.ses.sendRequest(m);
            inFlight++;
          }
          if (entry.req.encodedFully) {
            tasks.poll();
          }
        }
      } catch (Exception e) {
        tasks.poll();
        String errorMsg;
        if (entry.req != null) {
          errorMsg = String.format("Failed to send RPC %s from %s: %s", entry.req.msg,
            entry.ses, e.getMessage());
        } else {
          errorMsg = String.format("Failed to send empty msg from %s (count%d): %s", entry.ses, entry.count, e.getMessage());
        }
        logger.error(errorMsg, e.getMessage());
      }
    }
    if (prevMsgToUse != null) {
      msgPool.releaseMsg(prevMsgToUse);
    }
  }

  // msg can be null
  public Msg getMsg() {
    if (msgPool.isEmpty()) {
      //TimerStats.addRecord("Client empty msgpool", 1);
      return null;
    }
    Msg msg = msgPool.getMsg();
    return msg;
  }

  public EventQueueHandler getEqh() {
    return eqh;
  }
  
  public void retunrMsg(Msg msg) {
    //msgPool.releaseMsg(msg);
    msg.resetPositions();
    sendRequests(msg);
  }

  public void addTask(RdmaMessage req, ClientSession ses, int counter) {
    logger.debug(this+" addTask, req="+req+" ses="+ses);
    tasks.add(new Task(req, ses, counter));
    if (inFlight < SEND_TH) {
      eqh.breakEventLoop();
    }
  }

  public void updateClosed(RdmaClientSession rdmaClientSession) {
    sessions.remove(rdmaClientSession);
    checkIfCanClose();
  }
  
  private void checkIfCanClose() {
    if (sessions.size() == 0 && CLOSE) {
      eqh.breakEventLoop();
    }
  }

  public void requestToClose() {
    CLOSE = true;
    checkIfCanClose();
  }
  
  public void addSession(RdmaClientSession ses) {
    sessions.add(ses);
  }
  
  class Task {
    public RdmaMessage req;
    public ClientSession ses;
    public int count;
    public Task(RdmaMessage req, ClientSession ses, int count) {
      this.req = req;
      this.ses = ses;
      this.count = count;
    }
  }
}
