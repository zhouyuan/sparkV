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

package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.adapter.parquet.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.schema.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.vectorized.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;

public class VectorizedParquetArrowReader extends VectorizedParquetRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedParquetArrowReader.class);
  private ParquetReaderJniWrapper reader_handler;
  private ParquetReader reader = null;
  private BufferAllocator allocator;
  private String path;
  private long capacity;
  private VectorSchemaRoot schemaRoot = null;

  private long lastReadLength = 0;
  private int numLoaded = 0;
  private int numReaded = 0;
  private long totalLength;

  private List<FieldVector> vector_list;
  private ColumnarBatch last_columnar_batch;

  private StructType sourceSchema;
  private StructType readDataSchema;

  private long[] metrics = new long[5];

  public VectorizedParquetArrowReader(
      ParquetReaderJniWrapper reader_handler,
      BufferAllocator allocator,
      String path,
      TimeZone convertTz,
      boolean useOffHeap,
      int capacity,
      StructType sourceSchema,
      StructType readDataSchema
    ) {
    super(convertTz, useOffHeap, capacity);
    this.capacity = capacity;
    this.path = path;
    this.allocator = allocator;
    this.reader_handler = reader_handler;

    this.sourceSchema = sourceSchema;
    this.readDataSchema = readDataSchema;
  }

  @Override
  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException, UnsupportedOperationException {
    ParquetInputSplit split = (ParquetInputSplit)inputSplit;
    this.totalLength = split.getLength();

    int ordinal = 0;
    int cur_index = 0;

    int[] column_indices = new int[readDataSchema.size()];
    List<String> targetSchema = Arrays.asList(readDataSchema.names());
    for (String fieldName: sourceSchema.names()) {
      if (targetSchema.contains(fieldName)) {
        column_indices[cur_index++] = ordinal;
      }
      ordinal++;
    }
    LOG.info("column_indices is " + Arrays.toString(column_indices));

    this.reader = new ParquetReader(
      reader_handler, path, column_indices, split.getStart(), split.getEnd(), capacity);
    long zero = 0;
  }

  @Override
  public void initialize(String path, List<String> columns) throws IOException,
      UnsupportedOperationException {
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    return nextBatch();
  }

  @Override
  public boolean nextBatch() throws IOException {
    if (last_columnar_batch != null) {
      last_columnar_batch.close();
    }
    long start = System.nanoTime();
    if (schemaRoot == null) {
      Schema schema = reader.getSchema();
      schemaRoot = VectorSchemaRoot.create(schema, allocator);
    }
    vector_list = reader.readNextVectors(schemaRoot);
    if (vector_list == null) {
      lastReadLength = 0;
      return false;
    }
    lastReadLength = reader.lastReadLength();
    numLoaded += lastReadLength;
    metrics[4] = (System.nanoTime() - start);
    metrics[0] += metrics[4];
    metrics[3] += 1;

    return true;
  }

  @Override
  public Object getCurrentValue() {
    long start = System.nanoTime();
    if (numReaded == numLoaded) {
      return null;
    }
    numReaded += lastReadLength;
    int length = Math.toIntExact(lastReadLength);
    ArrowWritableColumnVector[] columnVectors =
      ArrowWritableColumnVector.loadColumns(length, vector_list);
    metrics[1] += (System.nanoTime() - start);
    last_columnar_batch = new ColumnarBatch(columnVectors, length, metrics);
    return last_columnar_batch;
  }

  @Override
  public void close() throws IOException {
    if (last_columnar_batch != null) {
      last_columnar_batch.close();
    }
    if (reader != null) {
      String[] metrics_toString = new String[5];
      metrics_toString[0] = new String("Fetch NextBatch From HDFS Parquet spent " +
        TimeUnit.NANOSECONDS.toMillis(metrics[0]) + " ms.");
      metrics_toString[2] = new String("Evaluate columnarBatch spent " +
        TimeUnit.NANOSECONDS.toMillis(metrics[2]) + " ms.");
      metrics_toString[3] = new String("Loaded " + metrics[3] + " columnarBatch.");
      LOG.info("File " + path + " \nhas metrics(ns) as " + Arrays.toString(metrics_toString));

      reader.close();
      reader = null;
    }
  }

  @Override
  public float getProgress() {
    return (float) (numReaded/totalLength);
  }

}

