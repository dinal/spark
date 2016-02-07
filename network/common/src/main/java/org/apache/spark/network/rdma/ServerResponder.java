package org.apache.spark.network.rdma;

import org.accelio.jxio.Msg;
import org.accelio.jxio.ServerSession;

public interface ServerResponder {
  public void respond(ServerSession session, Msg msgs);
}
