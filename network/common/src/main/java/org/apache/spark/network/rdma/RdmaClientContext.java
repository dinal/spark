package org.apache.spark.network.rdma;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.accelio.jxio.ClientSession;
import org.accelio.jxio.EventName;
import org.accelio.jxio.EventQueueHandler;
import org.accelio.jxio.EventReason;
import org.accelio.jxio.Msg;
import org.accelio.jxio.MsgPool;
import org.accelio.jxio.jxioConnection.Constants;
import org.accelio.jxio.jxioConnection.impl.JxioResourceManager;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.client.ChunkFetchFailureException;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamChunkId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaClientContext implements Runnable, MessageProvider {

  private final Logger logger = LoggerFactory.getLogger(RdmaClientContext.class);
  private final int CLIENT_BUF_COUNT = 20;
  private final int REQUEST_BATCH = 1;
  private final URI uri;
  private final EventQueueHandler eqh;
  private final ClientSession cs;
  private final MsgPool msgPool;
  private final int CHUNK_BATCH = 10;
  private final Thread contextThread;
  private boolean established = false;
  private boolean notifyClose = false;
  private boolean closed = false;
  private boolean sessionClosed = false;
  private HashMap<Long, RpcResponseCallback> rpcCallbacks =
      new HashMap<Long, RpcResponseCallback>();
  private HashMap<StreamChunkId, ChunkReceivedCallback> chunkCallbacks =
      new HashMap<StreamChunkId, ChunkReceivedCallback>();
  private ConcurrentLinkedQueue<RdmaMessage> tasks = new ConcurrentLinkedQueue<RdmaMessage>();
  private ByteBuffer proccessedResp = null;
  private Message.Type proccessedRespType;
  private int reqHandled = 0;
  private long dataRecieved = 0;
  private int timesReseted = 1;
  private long msgsSent = 0;

  public RdmaClientContext(URI uri) {
    this.uri = uri;
    eqh = new EventQueueHandler(null);
    msgPool = new MsgPool(CLIENT_BUF_COUNT, Constants.MSGPOOL_BUF_SIZE, Constants.MSGPOOL_BUF_SIZE);
    cs = new ClientSession(eqh, uri, new ClientCallbacks());
    eqh.runEventLoop(1, EventQueueHandler.INFINITE_DURATION); // wait for
                                                              // established
                                                              // event
    assert (established);
    contextThread = new Thread(this);
    contextThread.start();
  }

  public void run() {
    while (!notifyClose && !sessionClosed) {
      sendRequests();
      eqh.runEventLoop(EventQueueHandler.INFINITE_EVENTS, 1000);
    }
    logger.info("closing client, sent "+msgsSent+", recieved "+dataRecieved+" data bytes, different sessions "+timesReseted);
    if (!sessionClosed) {
      cs.close();
      eqh.runEventLoop(EventQueueHandler.INFINITE_EVENTS, EventQueueHandler.INFINITE_DURATION);
    }
    eqh.stop();
    eqh.close();
    msgPool.deleteMsgPool();
    closed = true;
    synchronized (this) {
      this.notify();
    }
  }

  private void sendRequests() {
    if (proccessedResp == null) {
      while (!tasks.isEmpty()) {
        RdmaMessage entry = null;
       // logger.info(this + "tasks peeking:"+ tasks.peek()+" tasks:"+tasks);
        entry = tasks.peek();
        try {
          List<Msg> msgsToSend = entry.encode(this);
          if (msgsToSend.isEmpty()) {
            //logger.info(this+" no msgs to send, breaking "+entry.msg);
            break;
          }
          
      //    logger.info(this + " Sending request {} to {} with msgs {}",
       //       entry, uri.getHost(), msgsToSend);
          
          for (Msg m : msgsToSend) {
            msgsSent++;
            cs.sendRequest(m);
          }
          if (entry.encodedFully) {
         //   logger.info(this + "tasks before polling :"+tasks);
            tasks.poll();
        //    logger.info(this + "tasks after polling :"+tasks);
          }
        } catch (Exception e) {
          tasks.poll();
          String errorMsg = String.format("Failed to send RPC %s to %s: %s", entry.msg,
              uri.getHost(), e.getMessage());
          logger.error(errorMsg, e.getMessage());
          // close();
          try {
            if (entry.msg instanceof RpcRequest) {
              rpcCallbacks.get(((RpcRequest)entry.msg).requestId).onFailure(new IOException(errorMsg, e.getCause()));
            } else if (entry.msg instanceof ChunkFetchRequest) {
              chunkCallbacks.get(((ChunkFetchRequest)entry.msg).streamChunkId).onFailure(
                  ((ChunkFetchRequest) entry.msg).streamChunkId.chunkIndex,
                  new IOException(errorMsg, e.getCause()));
            } else {
              logger.error("Failure sending unknown type", entry.msg);
            }
          } catch (Exception e1) {
            logger.error("Uncaught exception in RPC response callback handler!", e1);
          }
          
        }
      }
    }
    // waiting for chunks from server, sending empty buffers so the server have
    // were to write
    try {
      if (!chunkCallbacks.isEmpty()) {
        Msg m;
        for (int i=0; i< CHUNK_BATCH; i++) {
          if (msgPool.count() > 0) {
            m = msgPool.getMsg();
            msgsSent++;
            cs.sendRequest(m);
          } else {
            break;
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error sending msg " + e);
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

  private final class ClientCallbacks implements ClientSession.Callbacks {

    public void onMsgError(Msg msg, EventReason reason) {
      logger.debug(this.toString() + " onMsgErrorCallback, " + reason);
      msgPool.releaseMsg(msg);
    }

    public void onSessionEstablished() {
      established = true;
      logger.info(RdmaClientContext.this+" "+cs.toString() + " session established with host " + uri.getHost());
    }

    public void onSessionEvent(EventName event, EventReason reason) {
      logger.info(RdmaClientContext.this + " onSessionEvent " + event);
      if (event == EventName.SESSION_CLOSED || event == EventName.SESSION_ERROR
          || event == EventName.SESSION_REJECT) { // normal exit
        sessionClosed = true;
        eqh.breakEventLoop();
      }
    }

    public void onResponse(Msg m) {
     // logger.info(RdmaClientContext.this + "client onResponse m=" + m);
      ByteBuffer msgIn = m.getIn();
      dataRecieved += msgIn.limit();
      if (m.getIn().limit() == 0) {
        // response for UploadBlock that didn't fit into 1 buffer
        msgPool.releaseMsg(m);
        return;
      }
      if (proccessedResp == null) {
        //new response, not in the middle of getting chunk
        long dataSize = msgIn.getLong();
        proccessedRespType = Message.Type.decode(msgIn);
        if (dataSize <= msgIn.capacity()) {
          processResponse(proccessedRespType, m);
          return;
        } else {
          proccessedResp = ByteBuffer.allocate((int) dataSize - RdmaMessage.HEADER_LENGTH);
        }
      }
      proccessedResp.put(msgIn);
      if (proccessedResp.hasRemaining()) {
        // not finished yet
        msgPool.releaseMsg(m);
        return;
      }
      processResponse(proccessedRespType, m);

    }

    private void processResponse(Message.Type type, Msg msg) {
    //  logger.info(RdmaClientContext.this + " processResponse type=" + type + " msg=" + msg);
      ByteBuffer buf;
      boolean shortMsg = true;
      try {
        if (proccessedResp != null) {
          proccessedResp.position(0);
          buf = proccessedResp;
          shortMsg = false;
        } else { 
          buf = msg.getIn();
        }
        switch (type) {
        case RpcResponse:
          RpcResponse rpcResp = RpcResponse.decode(buf);
          msgPool.releaseMsg(msg);
          RpcResponseCallback rpcListener = rpcCallbacks.get(rpcResp.requestId);
          if (rpcListener == null) {
            logger.warn(RdmaClientContext.this + 
                " Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
                rpcResp.requestId, uri, rpcResp.body().size());
          } else {
            rpcCallbacks.remove(rpcResp.requestId);
            rpcListener.onSuccess(rpcResp.body().nioByteBuffer());
          }
          break;
        case RpcFailure:
          RpcFailure rpcFail = RpcFailure.decode(buf);
          msgPool.releaseMsg(msg);
          RpcResponseCallback rpcFailListener = rpcCallbacks.get(rpcFail.requestId);
          if (rpcFailListener == null) {
            logger.warn(RdmaClientContext.this + " Ignoring response for RPC {} from {} ({}) since"
                + " it is not outstanding",rpcFail.requestId, uri, rpcFail.errorString);
          } else {
            rpcCallbacks.remove(rpcFail.requestId);
            rpcFailListener.onFailure(new RuntimeException(rpcFail.errorString));
          }
          break;
        case ChunkFetchFailure:
          ChunkFetchFailure chunkFail = ChunkFetchFailure.decode(buf);
          msgPool.releaseMsg(msg);
          ChunkReceivedCallback chunkFailListener = chunkCallbacks.get(chunkFail.streamChunkId);
          if (chunkFailListener == null) {
            logger.warn(RdmaClientContext.this + " Ignoring response for block {} from {}"
                + " ({})" + " since it is not outstanding",chunkFail.streamChunkId, uri,
                  chunkFail.errorString);
          } else {
            chunkCallbacks.remove(chunkFail.streamChunkId);
            chunkFailListener.onFailure(chunkFail.streamChunkId.chunkIndex,
                new ChunkFetchFailureException("Failure while fetching " + chunkFail.streamChunkId
                    + ": " + chunkFail.errorString));
          }
          break;
        case ChunkFetchSuccess:
       //   logger.info(RdmaClientContext.this + " ChunkFetchSuccess decoding: " + buf +
      //		   " proccessedResp=" + proccessedResp);
          ChunkFetchSuccess chunkResp;
          if (shortMsg) {
            chunkResp = ChunkFetchSuccess.decode(msg);
          } else {
            chunkResp = ChunkFetchSuccess.decode(buf);
            msgPool.releaseMsg(msg);
          }
          ChunkReceivedCallback chunkListener = chunkCallbacks.get(chunkResp.streamChunkId);
          if (chunkListener == null) {
            logger.warn(RdmaClientContext.this + " Ignoring response for block {} from {}"
                + " since it is not outstanding",chunkResp.streamChunkId, uri);
          } else {
            chunkCallbacks.remove(chunkResp.streamChunkId);
            chunkListener.onSuccess(chunkResp.streamChunkId.chunkIndex, chunkResp.body());
          }
          chunkResp.body().release();
          break;
        default:
          logger.error(RdmaClientContext.this + " unknown response " + type);
        }
        proccessedResp = null;
      } catch(IOException e) {
        logger.error(RdmaClientContext.this + " exception while decoding msg type " + type +" Msg="+msg
            +" "+e.getMessage());
      }
     
    }
  }

  // can we use this client or is it used by other thread
  public boolean isActive() {
    return !(tasks.isEmpty() && rpcCallbacks.isEmpty() && chunkCallbacks.isEmpty()
        && (reqHandled != 0));
  }

  public void close() {
    if (closed) return;
    notifyClose = true;
    eqh.breakEventLoop();
    synchronized (this) {
      while (!closed) {
        try {
          //wait until jxio closes the connection
          wait(100);
        } catch (InterruptedException e) {
          logger.error("exception while waiting to close "+e);
        }
      }
    }
  }

  public void write(RdmaMessage req, long id, RpcResponseCallback respCallbacks) {
    rpcCallbacks.put(id, respCallbacks);
  //  logger.info(this +" 1added req to Q : "+req.msg+" tasks before: "+tasks);
    tasks.add(req);
 //   logger.info(this +"1tasks after: "+tasks);
    reqHandled++;
    if (tasks.size() > REQUEST_BATCH) {
      eqh.breakEventLoop();
    }
  }

  public void write(RdmaMessage req, StreamChunkId id, ChunkReceivedCallback respCallbacks) {
    chunkCallbacks.put(id, respCallbacks);
    reqHandled++;
 //   logger.info(this +" added req to Q : "+req.msg+" tasks before: "+tasks);
    tasks.add(req);
 //   logger.info(this +"tasks after: "+tasks);
    if (tasks.size() > REQUEST_BATCH) {
      eqh.breakEventLoop();
    }
  }

  public void reset() {
    timesReseted++;
    rpcCallbacks.clear();
    chunkCallbacks.clear();
    tasks.clear();
    proccessedResp = null;
    proccessedRespType = null;
    reqHandled = 0;
  }

  public boolean isConnected() {
    return established && !notifyClose && !sessionClosed;
  }
  
  /*private ChunkReceivedCallback retriveChunkCallback(StreamChunkId key) {
    for (StreamChunkId k : chunkCallbacks.keySet()) {
      if (k.equals(key)) {
        return chunkCallbacks.get(k);
      }
    }
    return null;
  }*/
}
