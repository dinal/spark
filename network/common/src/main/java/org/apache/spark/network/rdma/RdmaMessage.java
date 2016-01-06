package org.apache.spark.network.rdma;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import org.accelio.jxio.Msg;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.rdma.MessageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//message for input and output
public class RdmaMessage {
  private final Logger logger = LoggerFactory.getLogger(RdmaMessage.class);
  public static final int HEADER_LENGTH = 9; // HEADER=8 LONG, TYPE=1 BYTE
  public final Message msg;
  public final long id;
  public boolean encodedFully = false;
  public boolean decodedFully = false;
  private final int msgSize;
  private int encodedSize = 0;

  public RdmaMessage(Message msg, long id) {
    this.msg = msg;
    this.id = id;
    if (msg.type() == Message.Type.ChunkFetchSuccess) {
      msgSize = HEADER_LENGTH + msg.encodedLength() + (int)((ChunkFetchSuccess)msg).buffer.size();
    } else {
      msgSize = HEADER_LENGTH + msg.encodedLength();
    }
  }

  public List<Msg> encode(MessageProvider provider) {
    if (encodedFully) {
      logger.error("Message was already encoded");
      return null;
    }
    List<Msg> encodedData = new LinkedList<Msg>();
    Message.Type msgType = msg.type();
    logger.debug(this+" encode going to msgType=" + msgType + " msg=" + msg + " msgSize=" + msgSize);
    while (encodedSize < msgSize) {
      Msg m = provider.getMsg();
      if (m == null) {
        // can't encode anymore data, send partial
        break;
      } else {
        ByteBuffer buf = m.getOut();
        if (encodedSize == 0) {
          // first time in, need to write header and type
          buf.putLong(msgSize);
          msgType.encode(buf);
          encodedSize += HEADER_LENGTH; // long + byte
          encodePartial(msgType, true, buf);
        } else {
          encodePartial(msgType, false, buf);
        }
        encodedData.add(m);
      }
    }
    if (msgSize == encodedSize) {
      encodedFully = true;
    }
    return encodedData;
  }

  // encodes only part of the msg that can fit into the buffer
  private void encodePartial(Message.Type type, boolean includeMsgHeader, ByteBuffer buf) {
    logger.debug(this +" encodePartial type=" + type + " msg=" + msg + " msgSize=" + msgSize);
    int sizeToWrite = 0;
    switch (type) {
    case RpcRequest:
      RpcRequest rpcMsg = (RpcRequest) msg;
      if (includeMsgHeader) {
        buf.putLong(rpcMsg.requestId);
        buf.putInt(rpcMsg.message.length);
        encodedSize += 12;// long + int
      }
      sizeToWrite = Math.min(buf.remaining(), msgSize - encodedSize);
      buf.put(rpcMsg.message, rpcMsg.message.length - (msgSize - encodedSize), sizeToWrite);
      encodedSize += sizeToWrite;
      break;
    case RpcResponse:
      RpcResponse rpcresp = (RpcResponse) msg;
      if (includeMsgHeader) {
        buf.putLong(rpcresp.requestId);
        buf.putInt(rpcresp.response.length);
        encodedSize += 12;// long + int
      }
      sizeToWrite = Math.min(buf.remaining(), msgSize - encodedSize);
      buf.put(rpcresp.response, rpcresp.response.length - (msgSize - encodedSize), sizeToWrite);
      encodedSize += sizeToWrite;
      break;
    case RpcFailure:
      RpcFailure rpcFail = (RpcFailure) msg;
      if (includeMsgHeader) {
        buf.putLong(rpcFail.requestId);
        buf.putInt(rpcFail.errorString.length());
        encodedSize += 12;// long + int
      }
      sizeToWrite = Math.min(buf.remaining(), msgSize - encodedSize);
      buf.put(rpcFail.errorString.getBytes(Charset.forName("UTF-8")), rpcFail.errorString.length()
          - (msgSize - encodedSize), sizeToWrite);
      encodedSize += sizeToWrite;
      break;
    case ChunkFetchRequest:
      ChunkFetchRequest chunkReq = (ChunkFetchRequest) msg;
      chunkReq.encode(buf);
      encodedSize += chunkReq.encodedLength();
      break;
    case ChunkFetchFailure:
      ChunkFetchFailure chunkFail = (ChunkFetchFailure) msg;
      if (includeMsgHeader) {
        chunkFail.streamChunkId.encode(buf);
        buf.putInt(chunkFail.errorString.length());
        encodedSize += chunkFail.streamChunkId.encodedLength() + 4;
      }
      sizeToWrite = Math.min(buf.remaining(), msgSize - encodedSize);
      buf.put(chunkFail.errorString.getBytes(Charset.forName("UTF-8")), chunkFail.errorString.length()
          - (msgSize - encodedSize), sizeToWrite);
      encodedSize += sizeToWrite;
      break;
    case ChunkFetchSuccess:
      ChunkFetchSuccess chunkResp = (ChunkFetchSuccess) msg;
      if (includeMsgHeader) {
        chunkResp.streamChunkId.encode(buf);
        encodedSize += chunkResp.streamChunkId.encodedLength();
      }
      try {
        ByteBuffer dataBuff = chunkResp.buffer.nioByteBuffer();
        sizeToWrite = Math.min(buf.remaining(), msgSize - encodedSize);
        dataBuff.position(dataBuff.capacity() - (msgSize - encodedSize));
        dataBuff.limit(dataBuff.position() + sizeToWrite);
        buf.put(dataBuff);
      } catch (Exception e) {
        logger.error("Error occurred while encoding msg "+e+" "+e.getStackTrace());
      }
      encodedSize += sizeToWrite;
      break;
    default:
      throw new RuntimeException("Msg type is not supported" + type);
    }
  }

  @Override
  public String toString() {
	  return Thread.currentThread() + " " + msg;
  }

}
