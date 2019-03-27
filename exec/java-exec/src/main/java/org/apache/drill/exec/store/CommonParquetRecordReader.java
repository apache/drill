/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.store.parquet.ParquetReaderStats;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;

public abstract class CommonParquetRecordReader extends AbstractRecordReader {
  protected final FragmentContext fragmentContext;

  public ParquetReaderStats parquetReaderStats = new ParquetReaderStats();

  protected OperatorContext operatorContext;

  protected ParquetMetadata footer;

  public CommonParquetRecordReader(ParquetMetadata footer, FragmentContext fragmentContext) {
    this.footer = footer;
    this.fragmentContext = fragmentContext;
  }

  public void updateRowgroupsStats(long numRowgroups, long rowgroupsPruned) {
    parquetReaderStats.numRowgroups.set(numRowgroups);
    parquetReaderStats.rowgroupsPruned.set(rowgroupsPruned);
  }

  public enum Metric implements MetricDef {
    NUM_ROWGROUPS,               // Number of rowgroups assigned to this minor fragment
    ROWGROUPS_PRUNED,            // Number of rowgroups pruned out at runtime
    NUM_DICT_PAGE_LOADS,         // Number of dictionary pages read
    NUM_DATA_PAGE_lOADS,         // Number of data pages read
    NUM_DATA_PAGES_DECODED,      // Number of data pages decoded
    NUM_DICT_PAGES_DECOMPRESSED, // Number of dictionary pages decompressed
    NUM_DATA_PAGES_DECOMPRESSED, // Number of data pages decompressed
    TOTAL_DICT_PAGE_READ_BYTES,  // Total bytes read from disk for dictionary pages
    TOTAL_DATA_PAGE_READ_BYTES,  // Total bytes read from disk for data pages
    TOTAL_DICT_DECOMPRESSED_BYTES, // Total bytes decompressed for dictionary pages (same as compressed bytes on disk)
    TOTAL_DATA_DECOMPRESSED_BYTES, // Total bytes decompressed for data pages (same as compressed bytes on disk)
    TIME_DICT_PAGE_LOADS,          // Time in nanos in reading dictionary pages from disk
    TIME_DATA_PAGE_LOADS,          // Time in nanos in reading data pages from disk
    TIME_DATA_PAGE_DECODE,         // Time in nanos in decoding data pages
    TIME_DICT_PAGE_DECODE,         // Time in nanos in decoding dictionary pages
    TIME_DICT_PAGES_DECOMPRESSED,  // Time in nanos in decompressing dictionary pages
    TIME_DATA_PAGES_DECOMPRESSED,  // Time in nanos in decompressing data pages
    TIME_DISK_SCAN_WAIT,           // Time in nanos spent in waiting for an async disk read to complete
    TIME_DISK_SCAN,                // Time in nanos spent in reading data from disk.
    TIME_FIXEDCOLUMN_READ,         // Time in nanos spent in converting fixed width data to value vectors
    TIME_VARCOLUMN_READ,           // Time in nanos spent in converting varwidth data to value vectors
    TIME_PROCESS;                  // Time in nanos spent in processing

    @Override public int metricId() {
      return ordinal();
    }
  }

  protected void closeStats(Logger logger, Path hadoopPath) {
    if (parquetReaderStats != null) {
      if ( operatorContext != null ) {
        parquetReaderStats.update(operatorContext.getStats());
      }
      parquetReaderStats.logStats(logger, hadoopPath);
      parquetReaderStats = null;
    }
  }
}
