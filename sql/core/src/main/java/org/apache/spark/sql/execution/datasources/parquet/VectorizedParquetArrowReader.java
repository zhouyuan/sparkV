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

import org.apache.arrow.adapter.builder.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.schema.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.vectorized.*;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.StructType;

public class VectorizedParquetArrowReader extends VectorizedParquetRecordReader {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedParquetArrowReader.class);
  private ParquetArrowReader sharedReader = null;
  private ParquetReader reader = null;
  private int numBatched = 0;
  private ColumnarBatch columnarBatch;
  private WritableColumnVector[] columnVectors;
  private long length;
  private long capacity;
  private VectorSchemaRoot schemaRoot = null;
  private int numLoaded = 0;
  private int numReaded = 0;
  private long lastReadLength = 0;
  private List<FieldVector> vector_list;
  private ParquetInputSplit split;
  private Schema schema;
  private int[] column_indices;
  private long[] metrics = new long[5];

  public VectorizedParquetArrowReader(
      ParquetArrowReader sharedReader,
      TimeZone convertTz,
      boolean useOffHeap,
      int capacity,
      StructType sourceSchema,
      StructType readDataSchema
    ) {
    super(convertTz, useOffHeap, capacity);
    this.sharedReader = sharedReader;
    this.sharedReader.retain();
    this.capacity = capacity;
    int ordinal = 0;
    int cur_index = 0;
    List<String> targetSchema = Arrays.asList(readDataSchema.names());
    this.column_indices = new int[targetSchema.size()];
    for (String fieldName: sourceSchema.names()) {
      if (targetSchema.contains(fieldName)) {
        this.column_indices[cur_index++] = ordinal;
      }
      ordinal++;
    }
  }

  @Override
  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException, UnsupportedOperationException {
    ParquetInputSplit split = (ParquetInputSplit)inputSplit;
    this.length = split.getLength();

    //LOG.info("configuration is " + configuration);
    LOG.info("column_indices is " + Arrays.toString(this.column_indices));

    this.reader = new ParquetReader(
      sharedReader.getHdfsReader(), column_indices, split.getStart(), split.getEnd(), capacity);
    this.schema = reader.getSchema();
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
    long start = System.nanoTime();
    if (schemaRoot == null) {
      schemaRoot = VectorSchemaRoot.create(schema, sharedReader.getAllocator());
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
    return new ColumnarBatch(columnVectors, length, metrics);
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
    String[] metrics_toString = new String[5];
    metrics_toString[0] = new String("Fetch NextBatch From HDFS Parquet spent " +
        TimeUnit.NANOSECONDS.toMillis(metrics[0]) + " ms.");
    metrics_toString[1] = new String("Convert Arrow Batch to ColumnarBatch spent " +
        TimeUnit.NANOSECONDS.toMillis(metrics[1]) + " ms.");
    metrics_toString[2] = new String("Evaluate columnarBatch spent " +
        TimeUnit.NANOSECONDS.toMillis(metrics[2]) + " ms.");
    metrics_toString[3] = new String("Loaded " + metrics[3] + " columnarBatch.");
    LOG.info("File " + sharedReader.getFilePath() + " \nhas metrics(ns) as " + Arrays.toString(metrics_toString));
      reader.close();
      reader = null;
    }
    if (sharedReader != null) {
      sharedReader.close();
      sharedReader = null;
    }
  }

  @Override
  public float getProgress() {
    return (float) (numReaded/length);
  }

  private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
    Map<K, Set<V>> setMultiMap = new HashMap<>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      Set<V> set = new HashSet<>();
      set.add(entry.getValue());
      setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
    }
    return Collections.unmodifiableMap(setMultiMap);
  }
}

