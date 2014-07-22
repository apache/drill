/**
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
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;

public class ParquetScanBatchCreator implements BatchCreator<ParquetRowGroupScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetScanBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, ParquetRowGroupScan rowGroupScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    String partitionDesignator = context.getConfig().getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    List<SchemaPath> columns = rowGroupScan.getColumns();

    List<RecordReader> readers = Lists.newArrayList();

    List<String[]> partitionColumns = Lists.newArrayList();
    List<Integer> selectedPartitionColumns = Lists.newArrayList();
    boolean selectAllColumns = false;

    if (columns == null || columns.size() == 0) {
      selectAllColumns = true;
    } else {
      Pattern pattern = Pattern.compile(String.format("%s[0-9]+", partitionDesignator));
      for (SchemaPath column : columns) {
        Matcher m = pattern.matcher(column.getAsUnescapedPath());
        if (m.matches()) {
          columns.remove(column);
          selectedPartitionColumns.add(Integer.parseInt(column.getAsUnescapedPath().toString().substring(partitionDesignator.length())));
        }
      }
    }


    FileSystem fs = rowGroupScan.getStorageEngine().getFileSystem().getUnderlying();
    
    // keep footers in a map to avoid re-reading them
    Map<String, ParquetMetadata> footers = new HashMap<String, ParquetMetadata>();
    int numParts = 0;
    for(RowGroupReadEntry e : rowGroupScan.getRowGroupReadEntries()){
      /*
      Here we could store a map from file names to footers, to prevent re-reading the footer for each row group in a file
      TODO - to prevent reading the footer again in the parquet record reader (it is read earlier in the ParquetStorageEngine)
      we should add more information to the RowGroupInfo that will be populated upon the first read to
      provide the reader with all of th file meta-data it needs
      These fields will be added to the constructor below
      */
      try {
        if ( ! footers.containsKey(e.getPath())){
          footers.put(e.getPath(),
              ParquetFileReader.readFooter( fs.getConf(), new Path(e.getPath())));
        }
        readers.add(
            new ParquetRecordReader(
                context, e.getPath(), e.getRowGroupIndex(), fs,
                rowGroupScan.getStorageEngine().getCodecFactoryExposer(),
                footers.get(e.getPath()),
                rowGroupScan.getColumns()
            )
        );
        if (rowGroupScan.getSelectionRoot() != null) {
          String[] r = rowGroupScan.getSelectionRoot().split("/");
          String[] p = e.getPath().split("/");
          if (p.length > r.length) {
            String[] q = ArrayUtils.subarray(p, r.length, p.length - 1);
            partitionColumns.add(q);
            numParts = Math.max(numParts, q.length);
          } else {
            partitionColumns.add(new String[] {});
          }
        } else {
          partitionColumns.add(new String[] {});
        }
      } catch (IOException e1) {
        throw new ExecutionSetupException(e1);
      }
    }

    if (selectAllColumns) {
      for (int i = 0; i < numParts; i++) {
        selectedPartitionColumns.add(i);
      }
    }

    return new ScanBatch(rowGroupScan, context, readers.iterator(), partitionColumns, selectedPartitionColumns);
  }
}
