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

package org.apache.spark.shuffle.rdma.writer.wrapper

import java.io.{File, InputStream}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import org.apache.spark.shuffle.rdma._
import org.apache.spark.shuffle.sort._

class RdmaWrapperShuffleData(
    shuffleId: Int,
    numPartitions: Int,
    rdmaShuffleManager: RdmaShuffleManager) extends Logging {
  val rdmaMappedFileByMapIdx = new ConcurrentHashMap[Int, RdmaMappedFile].asScala

  def getInputStreams(partitionId: Int): Seq[InputStream] = {
    rdmaMappedFileByMapIdx.map(
      _._2.getByteBufferForPartition(partitionId)).filter(_ != null)
      .map(new ByteBufferBackedInputStream(_)).toSeq
  }

  def getInputStreams(partitionId: Int, startMapIdx: Int, endMapIdx: Int): Seq[InputStream] = {

    rdmaMappedFileByMapIdx.filter(temp => startMapIdx <= temp._1 && temp._1 < endMapIdx).map(
      _._2.getByteBufferForPartition(partitionId)).filter(_ != null)
      .map(new ByteBufferBackedInputStream(_)).toSeq
  }

  def getInputStreams(startPartitionId: Int, endPartitionId: Int): Seq[InputStream] = {
    rdmaMappedFileByMapIdx.map(
      _._2.getByteBufferForPartition(startPartitionId, endPartitionId)).filter(_ != null)
      .map(new ByteBufferBackedInputStream(_)).toSeq
  }

  def dispose(): Unit = rdmaMappedFileByMapIdx.foreach(_._2.dispose())

  def newShuffleWriter(): Unit = {}

  def getRdmaMappedFileForMapId(mapIdx: Int): RdmaMappedFile = rdmaMappedFileByMapIdx(mapIdx)

  def getRdmaMappedFileForPartitionId(partitionId: Int): RdmaMappedFile =
    rdmaMappedFileByMapIdx(partitionId)

  def removeDataByMap(partitionId: Int): Unit = {
    rdmaMappedFileByMapIdx.remove(partitionId).foreach(_.dispose())
  }

  def wrapperShuffleDataFile(mapIdx: Int, mapId: Long,
                             lengths: Array[Long]): Unit = {
    val dataFile = rdmaShuffleManager.shuffleBlockResolver.getDataFile(shuffleId, mapId)

    synchronized {

      val rdmaFile = new RdmaMappedFile(dataFile,
        rdmaShuffleManager.rdmaShuffleConf.shuffleWriteBlockSize.toInt, lengths,
        rdmaShuffleManager.getRdmaBufferManager)
      // Overwrite and dispose of older file if already exists
      rdmaMappedFileByMapIdx.put(mapIdx, rdmaFile).foreach(_.dispose())
    }
  }

}


