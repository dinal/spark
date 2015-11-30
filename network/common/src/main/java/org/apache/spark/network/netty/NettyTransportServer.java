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

package org.apache.spark.network.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import org.apache.spark.network.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Server for the efficient, low-level streaming service.
 */
public class NettyTransportServer implements TransportServer {
  private final Logger logger = LoggerFactory.getLogger(NettyTransportServer.class);

  private final NettyTransportContext context;
  private final TransportConf conf;
  private final RpcHandler appRpcHandler;
  private final List<TransportServerBootstrap> bootstraps;
  private ServerBootstrap bootstrap;
  private ChannelFuture channelFuture;
  private int port = -1;

  /** Creates a TransportServer that binds to the given port, or to any available if 0. */
  public NettyTransportServer(
      NettyTransportContext context,
      int portToBind,
      RpcHandler appRpcHandler,
      List<TransportServerBootstrap> bootstraps) {
    this.context = context;
    this.conf = context.getConf();
    this.appRpcHandler = appRpcHandler;
    this.bootstraps = Lists.newArrayList(Preconditions.checkNotNull(bootstraps));

    try {
      init(portToBind);
    } catch (RuntimeException e) {
      JavaUtils.closeQuietly(this);
      throw e;
    }
  }

  public int getPort() {
    if (port == -1) {
      throw new IllegalStateException("Server not initialized");
    }
    return port;
  }

  private void init(int portToBind) {

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    EventLoopGroup bossGroup =
      NettyUtils.createEventLoop(ioMode, conf.serverThreads(), "shuffle-server");
    EventLoopGroup workerGroup = bossGroup;

    PooledByteBufAllocator allocator = NettyUtils.createPooledByteBufAllocator(
      conf.preferDirectBufs(), true /* allowCache */, conf.serverThreads());

    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NettyUtils.getServerChannelClass(ioMode))
      .option(ChannelOption.ALLOCATOR, allocator)
      .childOption(ChannelOption.ALLOCATOR, allocator);

    if (conf.backLog() > 0) {
      bootstrap.option(ChannelOption.SO_BACKLOG, conf.backLog());
    }

    if (conf.receiveBuf() > 0) {
      bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf());
    }

    if (conf.sendBuf() > 0) {
      bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf());
    }

    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        RpcHandler rpcHandler = appRpcHandler;
        for (TransportServerBootstrap bootstrap : bootstraps) {
          rpcHandler = bootstrap.doBootstrap(ch, rpcHandler);
        }
        context.initializePipeline(ch, rpcHandler);
      }
    });

    channelFuture = bootstrap.bind(new InetSocketAddress(portToBind));
    channelFuture.syncUninterruptibly();

    port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
    logger.debug("Shuffle server started on port :" + port);
  }

  @Override
  public void close() {
    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
      channelFuture = null;
    }
    if (bootstrap != null && bootstrap.group() != null) {
      bootstrap.group().shutdownGracefully();
    }
    if (bootstrap != null && bootstrap.childGroup() != null) {
      bootstrap.childGroup().shutdownGracefully();
    }
    bootstrap = null;
  }
}
