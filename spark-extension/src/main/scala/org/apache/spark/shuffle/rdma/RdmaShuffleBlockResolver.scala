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

package org.apache.spark.shuffle.rdma

import java.io.{File, InputStream}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver}
import org.apache.spark.shuffle.rdma.writer.wrapper.RdmaWrapperShuffleData
import org.apache.spark.shuffle.sort.RdmaShuffleManager

class RdmaShuffleBlockResolver(rdmaShuffleManager: RdmaShuffleManager)
    extends IndexShuffleBlockResolver(rdmaShuffleManager.conf) with Logging {
  private val rdmaShuffleDataMap = new ConcurrentHashMap[Int, RdmaWrapperShuffleData]

  def removeShuffle(shuffleId: Int): Unit = {
    rdmaShuffleDataMap.remove(shuffleId) match {
      case r: RdmaWrapperShuffleData => r.dispose()
      case null =>
    }
  }

  def getRdmaShuffleData(shuffleId: ShuffleId): RdmaWrapperShuffleData =
    rdmaShuffleDataMap.get(shuffleId)

  def removeDataByMap(shuffleId: Int, mapIdx: Int): Unit = {
    getRdmaShuffleData(shuffleId).removeDataByMap(mapIdx)
  }

  override def writeMetadataFileAndCommit(
    shuffleId: Int,
    mapId: Long,
    lengths: Array[Long],
    checksums: Array[Long],
    dataTmp: File): Unit = {

    super.writeMetadataFileAndCommit(shuffleId, mapId, lengths, checksums, dataTmp)

    val mapIdx = TaskContext.getPartitionId()
    val numPartitions = lengths.length
    rdmaShuffleDataMap.putIfAbsent(shuffleId,
      new RdmaWrapperShuffleData(shuffleId, numPartitions, rdmaShuffleManager))

    val rdmaShuffleData = getRdmaShuffleData(shuffleId)
    rdmaShuffleData.wrapperShuffleDataFile(mapIdx, mapId, lengths)


    val rdmaMapTaskOutput = rdmaShuffleData.
      asInstanceOf[RdmaWrapperShuffleData].
      getRdmaMappedFileForMapId(mapIdx).getRdmaMapTaskOutput

    rdmaShuffleManager.publishMapTaskOutput(shuffleId, mapId, mapIdx, rdmaMapTaskOutput)

  }

  override def stop(): Unit = {}

  def getLocalRdmaPartition(shuffleId: Int, partitionId : Int) : Seq[InputStream] = {
    rdmaShuffleDataMap.get(shuffleId) match {
      case r: RdmaWrapperShuffleData => r.getInputStreams(partitionId)
      case null => Seq.empty
    }
  }

  def getLocalRdmaPartition(shuffleId: Int, partitionId : Int,
                            startMapIdx: Int, endMapIdx: Int) : Seq[InputStream] = {
    rdmaShuffleDataMap.get(shuffleId) match {
      case r: RdmaWrapperShuffleData =>
        r.getInputStreams(partitionId, startMapIdx, endMapIdx)
      case null => Seq.empty
    }
  }


  def getLocalRdmaPartition(shuffleId: Int, startPartitionId : Int,
                            endPartitionId : Int) : Seq[InputStream] = {
    rdmaShuffleDataMap.get(shuffleId) match {
      case r: RdmaWrapperShuffleData => r.getInputStreams(startPartitionId, endPartitionId)
      case null => Seq.empty
    }
  }

}
