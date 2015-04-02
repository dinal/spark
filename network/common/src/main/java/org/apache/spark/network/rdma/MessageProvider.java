package org.apache.spark.network.rdma;

import org.accelio.jxio.Msg;

public interface MessageProvider {
  public Msg getMsg();
}
