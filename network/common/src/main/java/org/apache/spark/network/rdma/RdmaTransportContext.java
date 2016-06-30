/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.rdma;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.TransportContext;


public class RdmaTransportContext implements TransportContext{
  private final Logger logger = LoggerFactory.getLogger(RdmaTransportContext.class);

  private final TransportConf conf;
  private final RpcHandler rpcHandler;

  public RdmaTransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
  }

  @Override
  public TransportClientFactory createClientFactory() {
    return new RdmaTransportClientFactory(conf);
  }

  /** Create a server which will attempt to bind to a specific port. */
  @Override
  public TransportServer createServer(int port) {
    return new RdmaTransportServer(this, new InetSocketAddress(port));
  }

  /** Creates a new server, binding to any available ephemeral port. */
  @Override
  public TransportServer createServer() {
    return createServer(0);
  }

  @Override
  public RpcHandler getRpcHandler() {
    return rpcHandler;
  }
  
  @Override
  public TransportConf getConf() { return conf; }
}
