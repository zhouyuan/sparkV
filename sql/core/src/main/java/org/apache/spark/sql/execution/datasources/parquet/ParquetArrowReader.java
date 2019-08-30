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
import java.lang.Math;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.adapter.builder.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.execution.datasources.VectorizedArrowReaderHandler;

public class ParquetArrowReader {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetArrowReader.class);
  private VectorizedArrowReaderHandler handler;
  private BufferAllocator allocator;
  private String filePath;
  private HdfsReader hdfsReader;

  private final AtomicInteger bufRefCnt = new AtomicInteger(0);

  public ParquetArrowReader(
      ParquetReaderHandler reader_handler,
      BufferAllocator allocator,
      String filePath,
      VectorizedArrowReaderHandler handler) {
    this.filePath = filePath;
    this.handler = handler;
    this.allocator = allocator;
    this.hdfsReader = new HdfsReader(reader_handler, filePath);
  }
  
  public void retain() {
    bufRefCnt.addAndGet(1);
  }

  public void release() {
    bufRefCnt.addAndGet(-1);
  }

  public void close() {
    release();
    if (bufRefCnt.get() == 0) {
      hdfsReader.close();
    }
  }

  public HdfsReader getHdfsReader() {
    return hdfsReader;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public String getFilePath() {
    return filePath;
  }

}

