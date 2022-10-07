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

import java.io.InputStream

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReadMetricsReporter, ShuffleReader}
import org.apache.spark.shuffle.rdma.RdmaShuffleReader.wrapStreamMethod
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

private[spark] class RdmaShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  override def read(): Iterator[Product2[K, C]] = {

    val (blocksByAddressIterator1, blocksByAddressIterator2) =
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startMapIndex,
        endMapIndex, startPartition, endPartition).duplicate

    val mapIdToBlockIndex = blocksByAddressIterator1.flatMap{
      case (_, blocks) => blocks.map {
        case (blockId, _, mapIdx) => blockId match {
          case x: ShuffleBlockId =>
            (x.mapId.asInstanceOf[Long], mapIdx.asInstanceOf[Integer])
          case x: ShuffleBlockBatchId =>
            (x.mapId.asInstanceOf[Long], mapIdx.asInstanceOf[Integer])
          case _ => throw new SparkException("Unknown block")
        }
      }
    }.toMap

    val rdmaShuffleFetcherIterator = new RdmaShuffleFetcherIterator(
      context,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      handle.shuffleId,
      blocksByAddressIterator2.toSeq,
      mapIdToBlockIndex)

    val dummyShuffleBlockId = ShuffleBlockId(0, 0, 0)
    // Wrap the streams for compression based on configuration
    val wrappedStreams = rdmaShuffleFetcherIterator.filter(_ != null).map {inputStream =>
      serializerManager.wrapStream(dummyShuffleBlockId, inputStream)
    }

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }
  }
}

object RdmaShuffleReader {
  // Retrieve the correct function for backward compatibility between Spark versions:
  // 2.0.x, 2.1.x and 2.2.x
  private val wrapStreamMethod = if (SparkVersionSupport.minorVersion == 0) {
    classOf[SerializerManager].getDeclaredMethod("wrapForCompression", classOf[BlockId],
      classOf[InputStream])
  } else {
    classOf[SerializerManager].getDeclaredMethod("wrapStream", classOf[BlockId],
      classOf[InputStream])
  }
}
