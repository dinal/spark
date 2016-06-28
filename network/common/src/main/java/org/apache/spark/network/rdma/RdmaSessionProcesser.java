package org.apache.spark.network.rdma;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.accelio.jxio.EventName;
import org.accelio.jxio.EventReason;
import org.accelio.jxio.Msg;
import org.accelio.jxio.ServerSession;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.RequestMessage;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaSessionProcesser extends TransportRequestHandler implements MessageProvider {
  private final Logger logger = LoggerFactory.getLogger(RdmaSessionProcesser.class);
  public SessionServerCallbacks callbacks;
  private ServerSession session;
  private Msg currentMsg = null;
  private LinkedList<RdmaMessage> backlog;
  private ByteBuffer proccessedReq;
  private Message.Type proccessedReqType;
  private RdmaServerWorker executor;
  private long enterTime;

  public RdmaSessionProcesser(RdmaServerWorker worker, String address, RpcHandler handler) {
    super(address, null, handler, handler.getStreamManager());
    callbacks = new SessionServerCallbacks();
    backlog = new LinkedList<RdmaMessage>();
    executor = worker;
  }

  public final class SessionServerCallbacks implements ServerSession.Callbacks {

    @Override
    public boolean onMsgError(Msg arg0, EventReason arg1) {
      logger.error(session.toString()+" recieved onMsgError "+arg1);
      return false;
    }

    @Override
    public void onRequest(final Msg m) {
     // enterTime = System.nanoTime();
     // TimerStats.addRecord("Server recieve", m.getIn().limit());
      currentMsg = m;         
      processMsg(m);
    }

    @Override
    public void onSessionEvent(EventName arg0, EventReason arg1) {
      logger.debug(session.toString()+" recieved onSessionEvent "+arg0+" "+arg1);
      if (arg0 == EventName.SESSION_CLOSED) {
        logger.debug(Thread.currentThread()+" "+ this +" session closed, session:"+session);
      //  stats.printRecords();
        executor.updateClosed(session);
      }
    }
  }

  private void processMsg(Msg m) {
    logger.debug(RdmaSessionProcesser.this + " recieved msg " + m);
    ByteBuffer data = m.getIn();
    if (data.limit() == 0) {
      // this means we received a fetch request in the past
      // which didn't fit fully in one buffer
      encodeAndSend();
    } else {
      if (proccessedReq == null) {
        long dataLength = data.getLong();
        proccessedReqType = Message.Type.decode(data);
        if (dataLength <= data.capacity()) {
          // msg fits in 1 buffer, no need to concatenate buffers
          RequestMessage decoded = decode(proccessedReqType, data);
          logger.debug(RdmaSessionProcesser.this + " short decoded, going to handle " + decoded);
          handle(decoded);
          return;
        } else {
          // msg is longer than 1 buffer, need to copy
          proccessedReq = ByteBuffer.allocate((int) dataLength - RdmaMessage.HEADER_LENGTH);
        }
      }
      proccessedReq.put(data);
     // logger.info(this.session + " proccessedReq " + proccessedReq);
      if (proccessedReq.hasRemaining()) {
        // return empty, not all data is received yet
        encodeAndSend();
      } else {
        // got all data, can decode
        RequestMessage decoded = decode(proccessedReqType, proccessedReq);
        proccessedReq = null;
    //    logger.info(this.session + " long msg= "+m+" decoded, going to handle " + decoded);
        handle(decoded);
      }
    }
  }

  private RequestMessage decode(Message.Type msgType, ByteBuffer in) {
    switch (msgType) {
    case ChunkFetchRequest:
      return ChunkFetchRequest.decode(in);
    case RpcRequest:
      return RpcRequest.decode(in);
    default:
      throw new IllegalArgumentException("Unexpected message type: " + msgType);
    }
  }

  @Override
  protected void processFetchRequest(final ChunkFetchRequest req) {
	
	logger.trace("Received req from {} to fetch block {}", address, req.streamChunkId);
	ManagedBuffer buf;
	try {
	  buf = streamManager.getChunk(req.streamChunkId.streamId, req.streamChunkId.chunkIndex);
	} catch (Exception e) {
	  logger.error(String.format(
	    "%s Error opening block %s for request from %s", this.session ,req.streamChunkId, address), e);
	  respond(new ChunkFetchFailure(req.streamChunkId, Thread.currentThread().getStackTrace().toString()));
	  return;
	}
	respond(new ChunkFetchSuccess(req.streamChunkId, buf));
  }
  
  @Override
  public void respond(Encodable result) {   
    logger.debug(this.session + " adding to backlog "+(Message) result);
    RdmaMessage rdmaMsg = new RdmaMessage((Message) result);
    //TimerStats.addRecord("Server block size", rdmaMsg.msgSize);
    backlog.add(rdmaMsg);
    encodeAndSend();
  }

  private void encodeAndSend() {
    RdmaMessage rdmaMsg = backlog.peek();
    try {
      if (rdmaMsg != null) {
        List<Msg> msgsToSend = rdmaMsg.encode(this, null);
        // always will include only one msg since we are replaying to each msg
        // immediately, and not accumulating them
        assert msgsToSend.size() == 1;
        if (rdmaMsg.encodedFully) {
          backlog.poll();
        }
        Msg m = msgsToSend.get(0);
        logger.debug(RdmaSessionProcesser.this +" sending response entry="+rdmaMsg+" msg="+m);
        //TimerStats.addRecord("Server process time", System.nanoTime() - enterTime);
        session.sendResponse(m);
      } else {
        logger.debug(RdmaSessionProcesser.this +" sending empty entry="+rdmaMsg);
        //TimerStats.addRecord("Server process time", System.nanoTime() - enterTime);
        session.sendResponse(currentMsg);
      }
    } catch (Exception e) {
      logger.error("Error while encoding and sending msg " +e.getStackTrace().toString());
    }
  }

  public void setSession(ServerSession session) {
    this.session = session;
  }

  @Override
  public Msg getMsg() {
    Msg temp = currentMsg;
    currentMsg = null;
    return temp;
  }
  
  @Override
  public void channelUnregistered() {
    
  }
  
  public SessionServerCallbacks getSessionCallbacks() {
    return callbacks;
  }
}
