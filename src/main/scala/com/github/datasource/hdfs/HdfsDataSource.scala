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
package com.github.datasource.hdfs

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer}

import com.github.datasource.common.Pushdown
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation => ExprAgg}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/** A scan object that works on HDFS files.
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class HdfsScan(schema: StructType,
               options: util.Map[String, String],
               filters: Array[Filter], prunedSchema: StructType,
               pushedAggregation: Option[ExprAgg])
      extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")

  private def init(): Unit = {
    if (options.get("format") == "parquet") {
      // The default for parquet is to use column names and no casts since
      // parquet knows what the data types are.
      options.put("useColumnNames", "")
      options.put("DisableCasts", "")
    }
  }
  init()
  override def toBatch: Batch = this

  protected val pushdown = new Pushdown(schema, prunedSchema, filters,
                                        pushedAggregation, options)

  override def readSchema(): StructType = pushdown.readSchema
  private val maxPartSize: Long = (1024 * 1024 * 128)
  private var partitions: Array[InputPartition] = getPartitions()

  private def createPartitions(blockMap: Map[String, Array[BlockLocation]],
                               store: HdfsStore): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    var i = 0
    if (options.containsKey("partitions") &&
          options.get("partitions").toInt == 1) {
      // Generate one partition per file
      for ((fName, blockList) <- blockMap) {
        a += new HdfsPartition(index = 0, offset = 0, length = store.getLength(fName),
                               name = fName)
      }
    } else {
      // Generate one partition per file, per hdfs block
      for ((fName, blockList) <- blockMap) {
        // Generate one partition per hdfs block.
        for (block <- blockList) {
          a += new HdfsPartition(index = i, offset = block.getOffset, length = block.getLength,
                                 name = fName,
                                 last = (i == blockList.size - 1))
          i += 1
        }
      }
    }
    a.toArray
  }
  private def createPartitionsParquet(blockMap: Map[String, Array[BlockLocation]],
                                      store: HdfsStore): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    var i = 0
    val conf = new Configuration()
    val readOptions = HadoopReadOptions.builder(conf)
                                       .build()
    logger.trace("blockMap: {}", blockMap.mkString(", "))
    if ((options.containsKey("partitions") &&
         options.get("partitions").toInt == 1)) {
      // Generate one partition per file
      for ((fName, blockList) <- blockMap) {
        val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(fName), conf),
                                            readOptions)
        val parquetBlocks = reader.getFooter.getBlocks
        val parquetBlock = parquetBlocks.get(0)
        a += new HdfsPartition(index = 0, offset = parquetBlock.getStartingPos,
                               length = store.getLength(fName), // parquetBlock.getTotalByteSize,
                               name = fName,
                               rows = parquetBlock.getRowCount,
                               store.getModifiedTime(fName))
      }
    } else {
      // Generate one partition per file, per hdfs block
      for ((fName, blockList) <- blockMap) {
        val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(fName),
                                                                     conf), readOptions)
        val parquetBlocks = reader.getFooter.getBlocks

        // Generate one partition per row Group.
        for (i <- 0 to parquetBlocks.size - 1) {
          val parquetBlock = parquetBlocks.get(i)
          a += new HdfsPartition(index = i, offset = parquetBlock.getStartingPos,
                                 length = parquetBlock.getCompressedSize,
                                 name = fName,
                                 rows = parquetBlock.getRowCount,
                                 store.getModifiedTime(fName),
                                 last = (i == parquetBlocks.size - 1))
        }
      }
    }
    // logger.info(a.mkString(", "))
    a.toArray
  }
  private val sparkSession: SparkSession = SparkSession
      .builder()
      .getOrCreate()
  private val broadcastedHadoopConf = HdfsColumnarReaderFactory.getHadoopConf(sparkSession,
                                                                              pushdown.readSchema)
  private val sqlConf = sparkSession.sessionState.conf
  /** Returns an Array of S3Partitions for a given input file.
   *  the file is selected by options("path").
   *  If there is one file, then we will generate multiple partitions
   *  on that file if large enough.
   *  Otherwise we generate one partition per file based partition.
   *
   * @return array of S3Partitions
   */
  private def getPartitions(): Array[InputPartition] = {
    var store: HdfsStore = HdfsStoreFactory.getStore(pushdown, options,
                                                     sparkSession, new Configuration())
    val fileName = store.filePath
    val blocks : Map[String, Array[BlockLocation]] = store.getBlockList(fileName)
    options.get("format") match {
      case "parquet" => createPartitionsParquet(blocks, store)
      case _ => createPartitions(blocks, store)
    }
  }
  override def planInputPartitions(): Array[InputPartition] = {
    partitions
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    /* Use the HdfsPartitionReaderFactory in cases where
     * we will get back text that needs to be parsed.
     * We also use the text partition reader
     * in the case of parquet with csv and pushdownNeeded.
     * When we have parquet csv but no pushdownNeeded,
     * we will use the ColumnarPartitionReader to avoid the
     * pushdown alltogether.
     */
    if ((options.containsKey("format") &&
         !options.get("format").contains("parquet")) ||
        (options.get("path").contains("ndphdfs") &&
         options.containsKey("outputFormat") &&
         options.get("outputFormat").contains("csv") &&
         pushdown.isPushdownNeeded)) {
      new HdfsPartitionReaderFactory(pushdown, options,
                                     broadcastedHadoopConf)
    } else if (options.get("path").contains("ndphdfs") &&
               options.containsKey("outputFormat") &&
               options.get("outputFormat").contains("binary") &&
               pushdown.isPushdownNeeded) {
        new HdfsBinColPartitionReaderFactory(pushdown, options,
                                             broadcastedHadoopConf, sqlConf)
      } else {
      new HdfsColumnarPartitionReaderFactory(pushdown, options,
                                             broadcastedHadoopConf, sqlConf)
    }
  }
}

/** Creates a factory for creating HdfsPartitionReader objects
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param filters the array of filters to push down
 * @param prunedSchema the new array of columns after pruning
 * @param pushedAggregation the array of aggregations to push down
 */
class HdfsPartitionReaderFactory(pushdown: Pushdown,
                                 options: util.Map[String, String],
 sharedConf: Broadcast[org.apache.spark.util.SerializableConfiguration])
  extends PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  private val sparkSession: SparkSession = SparkSession
      .builder()
      .getOrCreate()
  logger.trace("Created")
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new HdfsPartitionReader(pushdown, options, partition.asInstanceOf[HdfsPartition],
                            sparkSession, sharedConf)
  }
}

/** PartitionReader of HdfsPartitions
 *
 * @param pushdown object handling filter, project and aggregate pushdown
 * @param options the options including "path"
 * @param partition the HdfsPartition to read from
 */
class HdfsPartitionReader(pushdown: Pushdown,
                          options: util.Map[String, String],
                          partition: HdfsPartition,
                          sparkSession: SparkSession,
 sharedConf: Broadcast[org.apache.spark.util.SerializableConfiguration])
  extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(getClass)
  val tc = TaskContext.get()
  // logger.info(s"Task id: ${tc.taskAttemptId()} Stage id: ${tc.stageId} " +
  //             s"Partition: ${partition.index}:${partition.name}")
  /* We setup a rowIterator and then read/parse
   * each row as it is asked for.
   */
  private var store: HdfsStore = HdfsStoreFactory.getStore(pushdown, options,
                                                           sparkSession, sharedConf.value.value)
  private var rowIterator: Iterator[InternalRow] = {
    if (options.get("format") == "parquet") {
      store.getRowIterParquet(partition)
    } else {
      store.getRowIter(partition)
    }
  }

  var index = 0
  def next: Boolean = {
    rowIterator.hasNext
  }
  def get: InternalRow = {
    val row = rowIterator.next
    if (((index % 500000) == 0) ||
        (!next)) {
      logger.info(s"get: partition: ${partition.index} ${partition.offset}" +
                  s" ${partition.length} ${partition.name} index: ${index}")
    }
    index = index + 1
    row
  }

  def close(): Unit = Unit
}
