package org.apache.spark.network.rdma;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.accelio.jxio.EventName;
import org.accelio.jxio.EventReason;
import org.accelio.jxio.Msg;
import org.accelio.jxio.ServerSession;
import org.apache.spark.network.protocol.ChunkFetchRequest;
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
  private ExecutorService executor;
  public SessionServerCallbacks callbacks;
  private ServerSession session;
  // private List<Msg> workingMsgs;
  private Msg currentMsg = null;
  private ServerResponder responder;
  private List<RdmaMessage> backlog;
  private ByteBuffer proccessedReq;
  private Message.Type proccessedReqType;

  public RdmaSessionProcesser(ServerResponder responder, ExecutorService exec, String address,
      RpcHandler handler) {
    super(address, null, handler);
    executor = exec;
    callbacks = new SessionServerCallbacks();
    backlog = new LinkedList<RdmaMessage>();
    this.responder = responder;
  }

  public final class SessionServerCallbacks implements ServerSession.Callbacks {

    @Override
    public boolean onMsgError(Msg arg0, EventReason arg1) {
      logger.error(session.toString()+" recieved onMsgError "+arg1);
      return false;
    }

    @Override
    public void onRequest(final Msg m) {
      logger.info(session + " onRequest " + m);
      executor.execute(new Runnable() {
        @Override
        public void run() {
          logger.info(session + " setting new msg " + m);
          currentMsg = m;
          processMsg(m);
        }
      });
    }

    @Override
    public void onSessionEvent(EventName arg0, EventReason arg1) {
      logger.info(session.toString()+" recieved onSessionEvent "+arg0+" "+arg1);
    }
  }

  private void processMsg(Msg m) {
    ByteBuffer data = m.getIn();
    logger.info(this.session + " recieved msg " + data);
    if (data.limit() == 0) {
      // this means we received a fetch request in the past
      // which didn't fit fully in one buffer
      encodeAndSend();
    } else {
      if (proccessedReq == null) {
        long dataLength = data.getLong();
        logger.info(this.session + " dataLength " + dataLength);
        proccessedReqType = Message.Type.decode(data);
        logger.info(this.session + " proccessedReqType " + proccessedReqType);
        if (dataLength <= data.capacity()) {
          // msg fits in 1 buffer, no need to concatenate buffers
          RequestMessage decoded = decode(proccessedReqType, data);
          logger.info(this.session + " decoded, going to handle " + decoded);
          handle(decoded);
          return;
        } else {
          // msg is longer than 1 buffer, need to copy
          proccessedReq = ByteBuffer.allocate((int) dataLength - RdmaMessage.HEADER_LENGTH);
        }
      }
      proccessedReq.put(data);
      logger.info(this.session + " proccessedReq " + proccessedReq);
      if (proccessedReq.hasRemaining()) {
        // return empty, not all data is received yet
        encodeAndSend();
      } else {
        // got all data, can decode
        RequestMessage decoded = decode(proccessedReqType, proccessedReq);
        proccessedReq = null;
        logger.info(this.session + " decoded, going to handle " + decoded);
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
  public void respond(Encodable result) {
    logger.info("adding to backlog "+(Message) result);
    backlog.add(new RdmaMessage((Message) result, 0));
    encodeAndSend();
  }

  private void encodeAndSend() {
    try {
      RdmaMessage rdmaMsg = backlog.get(0);
      logger.info("encodeAndSend rdmaMsg="+rdmaMsg);
      List<Msg> msgsToSend = rdmaMsg.encode(this);
      logger.info("encodeAndSend encoded="+msgsToSend);
      // always will include only one msg since we are replaying to each msg
      // immediately, and not accumulating them
      assert msgsToSend.size() == 1;
      if (rdmaMsg.encodedFully) {
        backlog.remove(rdmaMsg);
      }
      responder.respond(session, msgsToSend.get(0));
    } catch (IndexOutOfBoundsException e) {
      // nothing to send, return empty
      responder.respond(session, currentMsg);
      return;
    }
  }

  public void setSession(ServerSession session) {
    this.session = session;
  }

  @Override
  public Msg getMsg() {
    Msg temp = currentMsg;
    currentMsg = null;
    logger.info("getMsg returning="+temp);
    return temp;
  }
}
