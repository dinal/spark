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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.network.client.StreamCallback;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;

public abstract class TransportClient implements Closeable{
 
  private String clientId;

  public abstract boolean isActive();
 
  public abstract void fetchChunk(long streamId,
      final int chunkIndex,
      final ChunkReceivedCallback callback);
 
  public abstract long sendRpc(ByteBuffer message, RpcResponseCallback callback);
 
  public abstract void close();
  
  public abstract SocketAddress getSocketAddress();
  
  //Only for RPC use, not shuffle
  public abstract void stream(final String streamId, final StreamCallback callback);
  public abstract void send(ByteBuffer message);
  public abstract TransportResponseHandler getHandler();
  
  /**
   * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
   * a specified timeout for a response.
   */
  public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
    final SettableFuture<ByteBuffer> result = SettableFuture.create();

    sendRpc(message, new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        result.set(response);
      }

      @Override
      public void onFailure(Throwable e) {
        result.setException(e);
      }
    });

    try {
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
  

  /**
   * Returns the ID used by the client to authenticate itself when authentication is enabled.
   *
   * @return The client ID, or null if authentication is disabled.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Sets the authenticated client ID. This is meant to be used by the authentication layer.
   *
   * Trying to set a different client ID after it's been set will result in an exception.
   */
  public void setClientId(String id) {
    Preconditions.checkState(clientId == null, "Client ID has already been set.");
    this.clientId = id;
  }
}
