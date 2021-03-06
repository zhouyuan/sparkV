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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriter, VectorizedArrowHandler}
import org.apache.spark.sql.vectorized.ColumnarBatch

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
class ParquetOutputWriter(path: String, context: TaskAttemptContext)
  extends OutputWriter {

  private var recordWriter: RecordWriter[Void, InternalRow] = null
  private var recordColumnarWriter: RecordWriter[Void, ColumnarBatch] = null

  def createRecordWriter(): RecordWriter[Void, InternalRow] = {
    new ParquetOutputFormat[InternalRow]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        new Path(path)
      }
    }.getRecordWriter(context)
  }

  def createRecordColumnarWriter(): RecordWriter[Void, ColumnarBatch] = {
    VectorizedArrowHandler.getWriter().getParquetWriter(context, path)
  }

  override def write(row: InternalRow): Unit = {
    if (recordWriter == null) {
      recordWriter = createRecordWriter()
    }
    recordWriter.write(null, row)
  }

  override def write(columnarBatch: ColumnarBatch): Unit = {
    if (recordColumnarWriter == null) {
      recordColumnarWriter = createRecordColumnarWriter()
    }
    recordColumnarWriter.write(null, columnarBatch)
  }

  override def close(): Unit = if (recordColumnarWriter != null) {
    recordColumnarWriter.asInstanceOf[VectorizedParquetArrowWriter].close(context)
  } else {
    recordWriter.close(context)
  }
}
