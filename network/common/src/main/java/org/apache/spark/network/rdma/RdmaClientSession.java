package org.apache.spark.network.rdma;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.accelio.jxio.ClientSession;
import org.accelio.jxio.EventName;
import org.accelio.jxio.EventReason;
import org.accelio.jxio.Msg;
import org.accelio.jxio.jxioConnection.Constants;
import org.apache.spark.network.client.ChunkFetchFailureException;
import org.apache.spark.network.client.ChunkReceivedCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.Message.Type;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RdmaClientSession {
  
  private final Logger logger = LoggerFactory.getLogger(RdmaClientSession.class);
  private final ClientSession cs;
  private final RdmaClientContext ctx;
  private final URI uri;
  private boolean notifyClose;
  private boolean sessionClosed = false;
  private ByteBuffer proccessedResp = null;
  private Message.Type proccessedRespType;
  private HashMap<Long, RpcResponseCallback> rpcCallbacks = new HashMap<Long, RpcResponseCallback>();
  private HashMap<StreamChunkId, ChunkReceivedCallback> chunkCallbacks = new HashMap<StreamChunkId, ChunkReceivedCallback>();
  private long dataRecieved = 0;
  private int timesReseted = 1;
  private boolean sessionEstablished = false;
  private boolean working = true;
  private Object lock = new Object();
  
  public RdmaClientSession(URI uri, RdmaClientContext ctx) {
    
    logger.debug(this+" new RdmaClientSession uri="+uri);
    this.ctx = ctx;
    ctx.addSession(this);
    this.uri = uri;
    cs = new ClientSession(ctx.getEqh(), uri, new ClientCallbacks());
    synchronized (lock) {
      try {
        if (!sessionEstablished && !sessionClosed) {
          lock.wait();
        }
      } catch (InterruptedException e) {
        logger.error(this+" error while waiting for session established");
      }
    }
  }
  
  private final class ClientCallbacks implements ClientSession.Callbacks {

    public void onMsgError(Msg msg, EventReason reason) {
      logger.error(this.toString() + " onMsgErrorCallback, " + reason);
      ctx.retunrMsg(msg);
    }

    public void onSessionEstablished() {
      logger.debug(RdmaClientSession.this+" session established with host " + uri.getHost());
      synchronized (lock) {
        sessionEstablished = true;
        lock.notify();
      }
    }

    public void onSessionEvent(EventName event, EventReason reason) {
      if (sessionEstablished) {
        logger.debug(RdmaClientSession.this + " onSessionEvent " + event+" reason "+reason);
      } else {
        logger.error(RdmaClientSession.this + " onSessionEvent " + event+" reason "+reason);
      }
      if (event == EventName.SESSION_CLOSED || event == EventName.SESSION_ERROR
          || event == EventName.SESSION_REJECT) { // normal exit
        synchronized (lock) {
          sessionClosed = true;
          logger.debug("Closing client session, numUsed:"+timesReseted+", dataRecieved:"+dataRecieved);
          ctx.updateClosed(RdmaClientSession.this);
          lock.notify();
        }
      }
    }

    public void onResponse(Msg m) {
      logger.debug(RdmaClientSession.this +" onResponse "+m);
      ByteBuffer msgIn = m.getIn();
      dataRecieved += msgIn.limit();
      if (m.getIn().limit() == 0) {
        ctx.retunrMsg(m);
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
          double numEmptyMsgs = Math.ceil(((double)proccessedResp.limit() - msgIn.remaining()) / Constants.MSGPOOL_BUF_SIZE);
          ctx.addTask(null, cs, (int)numEmptyMsgs);
        }
      }
      proccessedResp.put(msgIn);
      if (proccessedResp.hasRemaining()) {
        // not finished yet
        ctx.retunrMsg(m);
        return;
      }
      processResponse(proccessedRespType, m);

    }

    private void processResponse(Message.Type type, Msg msg) {
      logger.debug(RdmaClientSession.this +" processResponse "+msg+" type:"+type);
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
          RpcResponseCallback rpcListener = rpcCallbacks.get(rpcResp.requestId);
          if (rpcListener == null) {
            logger.warn(RdmaClientSession.this + 
                " Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
                rpcResp.requestId, uri, rpcResp.body().size());
          } else {
            rpcCallbacks.remove(rpcResp.requestId);
            rpcListener.onSuccess(rpcResp.body().nioByteBuffer());
          }
          ctx.retunrMsg(msg);
          break;
        case RpcFailure:
          RpcFailure rpcFail = RpcFailure.decode(buf);
          RpcResponseCallback rpcFailListener = rpcCallbacks.get(rpcFail.requestId);
          if (rpcFailListener == null) {
            logger.warn(RdmaClientSession.this + " Ignoring response for RPC {} from {} ({}) since"
                + " it is not outstanding",rpcFail.requestId, uri, rpcFail.errorString);
          } else {
            rpcCallbacks.remove(rpcFail.requestId);
            rpcFailListener.onFailure(new RuntimeException(rpcFail.errorString));
          }
          ctx.retunrMsg(msg);
          break;
        case ChunkFetchFailure:
          ChunkFetchFailure chunkFail = ChunkFetchFailure.decode(buf);
          ChunkReceivedCallback chunkFailListener = chunkCallbacks.get(chunkFail.streamChunkId);
          if (chunkFailListener == null) {
            logger.warn(RdmaClientSession.this + " Ignoring response for block {} from {}"
                + " ({})" + " since it is not outstanding",chunkFail.streamChunkId, uri,
                  chunkFail.errorString);
          } else {
            chunkCallbacks.remove(chunkFail.streamChunkId);
            chunkFailListener.onFailure(chunkFail.streamChunkId.chunkIndex,
                new ChunkFetchFailureException("Failure while fetching " + chunkFail.streamChunkId
                    + ": " + chunkFail.errorString));
          }
          ctx.retunrMsg(msg);
          break;
        case ChunkFetchSuccess:
       //   logger.info(RdmaClientContext.this + " ChunkFetchSuccess decoding: " + buf +
      //       " proccessedResp=" + proccessedResp);
          ChunkFetchSuccess chunkResp;
          if (shortMsg) {
            chunkResp = ChunkFetchSuccess.decode(msg);
          } else {
            chunkResp = ChunkFetchSuccess.decode(buf);
          }
          ChunkReceivedCallback chunkListener = chunkCallbacks.get(chunkResp.streamChunkId);
          if (chunkListener == null) {
            logger.warn(RdmaClientSession.this + " Ignoring response for block {} from {}"
                + " since it is not outstanding",chunkResp.streamChunkId, uri);
          } else {
            chunkCallbacks.remove(chunkResp.streamChunkId);
            chunkListener.onSuccess(chunkResp.streamChunkId.chunkIndex, chunkResp.body());
          }
          chunkResp.body().release();
          if (!shortMsg) {
            ctx.retunrMsg(msg);
          }
          if (chunkCallbacks.isEmpty()) {
            working = false;
          }
          break;
        default:
          logger.error(RdmaClientSession.this + " unknown response " + type);
        }
        proccessedResp = null;
      } catch(IOException e) {
        logger.error(RdmaClientSession.this + " exception while decoding msg type " + type +" Msg="+msg
            +" "+e.getMessage());
      }
    }
  }


  public void write(RdmaMessage req, long id, RpcResponseCallback respCallbacks) {
    logger.debug(this+" "+Thread.currentThread()+" sending rpc");
    rpcCallbacks.put(id, respCallbacks);
    ctx.addTask(req, cs, 1);
  }

  public void write(RdmaMessage req, StreamChunkId id, ChunkReceivedCallback respCallbacks) {
    logger.debug(this+" "+Thread.currentThread()+" requesting chunk");
    chunkCallbacks.put(id, respCallbacks);
    ctx.addTask(req, cs, 1);
  }

  public void close() {
    notifyClose = true;
    if (!cs.getIsClosing()) {
      cs.close();
    }
  }

  public void reset() {
    logger.debug(this+" reset()");
    timesReseted++;
    rpcCallbacks.clear();
    chunkCallbacks.clear();
    proccessedResp = null;
    proccessedRespType = null;
    working = true;
  }

  public boolean isActive() {
    return !notifyClose && !sessionClosed && sessionEstablished;
  }
  
  public boolean isWorking() {
    return working;
  }
}
