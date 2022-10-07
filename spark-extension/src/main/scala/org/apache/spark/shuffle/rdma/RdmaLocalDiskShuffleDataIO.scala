/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.rdma

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO

/**
 * Rdma local disk IO plugin to handle logic of
 * writing to local disk and shuffle memory registration.
 */
case class RdmaLocalDiskShuffleDataIO(sparkConf: SparkConf)
  extends LocalDiskShuffleDataIO(sparkConf) with Logging {

  override def executor(): ShuffleExecutorComponents = {
    new RdmaLocalDiskShuffleExecutorComponents(sparkConf)
  }
}
