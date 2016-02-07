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

package org.apache.spark.network.rdma

import java.nio.ByteBuffer
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.network._
import org.apache.spark.network.server._
import org.apache.spark.network.netty._
import org.apache.spark.storage.{ BlockId, StorageLevel }
import org.apache.spark.util.Utils
import org.apache.spark.{ Logging, SecurityManager, SparkConf, SparkException }
import scala.concurrent.{ Future, Promise }
import org.apache.spark.network.shuffle._
import org.apache.spark.network.client.{ RpcResponseCallback, TransportClientFactory }
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.protocol._

/**
 * A [[BlockTransferService]] implementation based on JXIO library a custom
 * implementation using RDMA.
 */
final class RdmaBlockTransferService(conf: SparkConf, securityManager: SecurityManager)
  extends BlockTransferService with Logging {

  private var rdmaServer: TransportServer = _
  private var blockDataManager: BlockDataManager = _
  private val serializer = new JavaSerializer(conf)
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  private[this] var transportContext: TransportContext = _
  private[this] var appId: String = _
  private[this] var clientFactory: TransportClientFactory = _

  override def init(blockDataManager: BlockDataManager): Unit = {
    this.blockDataManager = blockDataManager
    val rpcHandler = new BlockRpcServer(conf.getAppId, serializer, blockDataManager)
    transportContext = new RdmaTransportContext(transportConf, rpcHandler)
    rdmaServer = createServer()
    appId = conf.getAppId
    clientFactory = transportContext.createClientFactory()
  }
  
  /** Creates and binds the TransportServer, possibly trying multiple ports. */
  private def createServer(): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server = transportContext.createServer(port)
      (server, server.getPort)
    }

    val portToTry = conf.getInt("spark.blockManager.port", 0)
    Utils.startServiceOnPort(portToTry, startService, conf, getClass.getName)._1
  }

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
          val client = clientFactory.createClient(host, port)
          new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener).start()
        }
      }

      val maxRetries = transportConf.maxIORetries()
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
  }

  override def hostName: String = Utils.localHostName()

  override def port: Int = rdmaServer.getPort()

  override def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel): Future[Unit] = {
    val result = Promise[Unit]()
    val client = clientFactory.createClient(hostname, port)

    // StorageLevel is serialized as bytes using our JavaSerializer. Everything else is encoded
    // using our binary protocol.
    val levelBytes = serializer.newInstance().serialize(level).array()

    // Convert or copy nio buffer into array in order to serialize it.
    val nioBuffer = blockData.nioByteBuffer()
    val array = if (nioBuffer.hasArray) {
      nioBuffer.array()
    } else {
      val data = new Array[Byte](nioBuffer.remaining())
      nioBuffer.get(data)
      data
    }

    client.sendRpc(new UploadBlock(appId, execId, blockId.toString, levelBytes, array).toByteBuffer,
      new RpcResponseCallback {
        override def onSuccess(response: ByteBuffer): Unit = {
          logTrace(s"Successfully uploaded block $blockId")
          result.success((): Unit)
        }
        override def onFailure(e: Throwable): Unit = {
          logError(s"Error while uploading block $blockId", e)
          result.failure(e)
        }
      })

    result.future
  }

  override def close(): Unit = {
    rdmaServer.close()
    clientFactory.close()
    logInfo(s"RdmaBlockTransferService closed!")
  }
}
