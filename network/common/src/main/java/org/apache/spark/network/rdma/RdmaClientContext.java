package org.apache.spark.network.rdma;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
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
  private boolean established = false;
  private boolean close = false;
  private ConcurrentMap<Long, RpcResponseCallback> rpcCallbacks =
      new ConcurrentHashMap<Long, RpcResponseCallback>();
  private ConcurrentMap<Long, ChunkReceivedCallback> chunkCallbacks =
      new ConcurrentHashMap<Long, ChunkReceivedCallback>();
  private ConcurrentLinkedQueue<RdmaMessage> tasks =
      new ConcurrentLinkedQueue<RdmaMessage>();
  private ByteBuffer proccessedResp = null;
  private Message.Type proccessedRespType;
  private int reqHandled = 0;
  private Thread contextThread;

  public RdmaClientContext(URI uri) {
    this.uri = uri;
    eqh = JxioResourceManager.getEqh();
    msgPool = JxioResourceManager.getMsgPool(CLIENT_BUF_COUNT, Constants.MSGPOOL_BUF_SIZE / 2,
        Constants.MSGPOOL_BUF_SIZE / 2);
    cs = new ClientSession(eqh, uri, new ClientCallbacks());
    eqh.runEventLoop(1, EventQueueHandler.INFINITE_DURATION); // wait for
                                                              // established
                                                              // event
    assert (established);
    contextThread = new Thread(this);
    contextThread.start();
  }

  public void run() {
    while (!close) {
      sendRequests();
      eqh.runEventLoop(EventQueueHandler.INFINITE_EVENTS, 1000);
    }
    cs.close();
    eqh.runEventLoop(EventQueueHandler.INFINITE_EVENTS, EventQueueHandler.INFINITE_DURATION);
    JxioResourceManager.returnEqh(eqh);
    JxioResourceManager.returnMsgPool(msgPool);
    synchronized (this) {
      this.notify();
    }
  }

  private void sendRequests() {
    if (proccessedResp == null) {
      for (RdmaMessage entry : tasks) {
        try {
          List<Msg> msgsToSend = entry.encode(this);
          if (msgsToSend.isEmpty()) 
            break;
          
          logger.info(Thread.currentThread() + " "+this + " Sending request {} to {} with msgs {}",
              entry, uri.getHost(), msgsToSend);
          
          for (Msg m : msgsToSend) {
            cs.sendRequest(m);
          }
          if (entry.encodedFully) {
            tasks.remove(entry);
          }
        } catch (Exception e) {
          tasks.remove(entry);
          String errorMsg = String.format("Failed to send RPC %s to %s: %s", entry.id,
              uri.getHost(), e.getCause());
          logger.error(errorMsg, e.getCause());
          // close();
          try {
            if (entry.msg instanceof RpcRequest) {
              rpcCallbacks.get(entry.id).onFailure(new IOException(errorMsg, e.getCause()));
            } else if (entry.msg instanceof ChunkFetchRequest) {
              chunkCallbacks.get(entry.id).onFailure(
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
        while ((m = msgPool.getMsg()) != null) {
          logger.info(Thread.currentThread() + " "+this + " Sending empty request {}", m);
          cs.sendRequest(m);
        }
      }
    } catch (Exception e) {
      logger.error("Error sending msg " + e);
    }
  }

  // msg can be null
  public Msg getMsg() {
    Msg msg = msgPool.getMsg();
    return msg;
  }

  private final class ClientCallbacks implements ClientSession.Callbacks {

    public void onMsgError(Msg msg, EventReason reason) {
      logger.info(this.toString() + " onMsgErrorCallback, " + reason);
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
        close = true;
        eqh.breakEventLoop();
      }
    }

    public void onResponse(Msg m) {
      logger.info(RdmaClientContext.this + "client onResponse m=" + m);
      ByteBuffer buf = m.getIn();
      if (buf.limit() == 0) {
        // response for UploadBlock that didn't fit into 1 buffer
        msgPool.releaseMsg(m);
        return;
      }
      if (proccessedResp == null) {
        long dataSize = buf.getLong();
        proccessedRespType = Message.Type.decode(buf);
        if (dataSize <= buf.capacity()) {
          processResponse(proccessedRespType, m);
          return;
        } else {
          proccessedResp = ByteBuffer.allocate((int) dataSize - RdmaMessage.HEADER_LENGTH);
        }
      }
      proccessedResp.put(buf);
      if (proccessedResp.hasRemaining()) {
        // not finished yet
        msgPool.releaseMsg(m);
        return;
      } else {
        processResponse(proccessedRespType, m);
      }

    }

    private void processResponse(Message.Type type, Msg msg) {
      logger.info(RdmaClientContext.this + " processResponse type=" + type + " msg=" + msg);
      ByteBuffer buf;
      boolean shortMsg = true;
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
              rpcResp.requestId, uri, rpcResp.response.length);
        } else {
          rpcCallbacks.remove(rpcResp.requestId);
          rpcListener.onSuccess(rpcResp.response);
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
        ChunkReceivedCallback chunkFailListener = retriveChunkCallback(
            chunkFail.streamChunkId.hashCode());
        if (chunkFailListener == null) {
          logger.warn(RdmaClientContext.this + " Ignoring response for block {} from {}"
              + " ({})" + " since it is not outstanding",chunkFail.streamChunkId, uri,
                chunkFail.errorString);
        } else {
          chunkCallbacks.remove(chunkFail.streamChunkId.hashCode());
          chunkFailListener.onFailure(chunkFail.streamChunkId.chunkIndex,
              new ChunkFetchFailureException("Failure while fetching " + chunkFail.streamChunkId
                  + ": " + chunkFail.errorString));
        }
        break;
      case ChunkFetchSuccess:
        logger.info(RdmaClientContext.this + " ChunkFetchSuccess decoding: " + buf +
    		   " proccessedResp=" + proccessedResp);
        ChunkFetchSuccess chunkResp;
        if (shortMsg) {
          chunkResp = ChunkFetchSuccess.decode(msg);
        } else {
          chunkResp = ChunkFetchSuccess.decode(buf);
          msgPool.releaseMsg(msg);
          /*ByteBuffer newBuf = ByteBuffer.allocate(buf.remaining());
          newBuf.put(buf);
          newBuf.position(0);
          chunkResp = ChunkFetchSuccess.decode(newBuf);
          msgPool.releaseMsg(msg);*/
        }
        ChunkReceivedCallback chunkListener = retriveChunkCallback(
            chunkResp.streamChunkId.hashCode());
        if (chunkListener == null) {
          logger.warn(RdmaClientContext.this + " Ignoring response for block {} from {}"
              + " since it is not outstanding",chunkResp.streamChunkId, uri);
        } else {
          chunkCallbacks.remove((long)chunkResp.streamChunkId.hashCode());
          chunkListener.onSuccess(chunkResp.streamChunkId.chunkIndex, chunkResp.buffer);
        }
        chunkResp.buffer.release();
        break;
      default:
        logger.error(RdmaClientContext.this + " unknown response " + type);
      }

      proccessedResp = null;
    }
  }

  // can we use this client or is it used by other thread
  public boolean isActive() {
    return !(tasks.isEmpty() && rpcCallbacks.isEmpty() && chunkCallbacks.isEmpty()
        && (reqHandled != 0));
  }

  public void close() {
    logger.info("Closing RDdmaClient");
    if (close) return;
    close = true;
    eqh.breakEventLoop();
    synchronized (this) {
      try {
        //wait until jxio closes the connection
        wait();
      } catch (InterruptedException e) {
        logger.error("exception while waiting to close "+e);
      }
    }
  }

  public void write(RdmaMessage req, RpcResponseCallback respCallbacks) {
    rpcCallbacks.put(req.id, respCallbacks);
    tasks.add(req);
    if (tasks.size() > REQUEST_BATCH) {
      eqh.breakEventLoop();
    }
  }

  public void write(RdmaMessage req, ChunkReceivedCallback respCallbacks) {
    chunkCallbacks.put(req.id, respCallbacks);
    reqHandled++;
    tasks.add(req);
    if (tasks.size() > REQUEST_BATCH) {
      eqh.breakEventLoop();
    }
  }

  public void reset() {
    rpcCallbacks.clear();
    chunkCallbacks.clear();
    tasks.clear();
    proccessedResp = null;
    proccessedRespType = null;
    reqHandled = 0;
  }

  public boolean isConnected() {
    return established && !close;
  }
  
  private ChunkReceivedCallback retriveChunkCallback(long key) {
    for (Long k : chunkCallbacks.keySet()) {
      if (k == key) {
        return chunkCallbacks.get(k);
      }
    }
    return null;
  }
}
