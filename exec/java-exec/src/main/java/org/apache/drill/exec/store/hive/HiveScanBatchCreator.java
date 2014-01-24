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
package org.apache.drill.exec.store.hive;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.List;

public class HiveScanBatchCreator implements BatchCreator<HiveSubScan> {

  @Override
  public RecordBatch getBatch(FragmentContext context, HiveSubScan config, List<RecordBatch> children) throws ExecutionSetupException {
    List<RecordReader> readers = Lists.newArrayList();
    Table table = config.table;
    List<InputSplit> splits = config.getInputSplits();
    List<Partition> partitions = config.partitions;
    if (partitions == null || partitions.size() == 0) {
      if (table.getSd().getInputFormat().equals(TextInputFormat.class.getCanonicalName()) &&
              table.getSd().getSerdeInfo().getSerializationLib().equals(LazySimpleSerDe.class.getCanonicalName()) &&
              config.getColumns() != null) {
        for (InputSplit split : splits) {
          readers.add(new HiveTextRecordReader(table, null, split, config.getColumns(), context));
        }
      } else {
        for (InputSplit split : splits) {
          readers.add(new HiveRecordReader(table, null, split, config.getColumns(), context));
        }
      }
    } else {
      int i = 0;
      if (table.getSd().getInputFormat().equals(TextInputFormat.class.getCanonicalName()) &&
              table.getSd().getSerdeInfo().getSerializationLib().equals(LazySimpleSerDe.class.getCanonicalName()) &&
              config.getColumns() != null) {
        for (InputSplit split : splits) {
          readers.add(new HiveTextRecordReader(table, partitions.get(i++), split, config.getColumns(), context));
        }
      } else {
        for (InputSplit split : splits) {
          readers.add(new HiveRecordReader(config.table, partitions.get(i++), split, config.getColumns(), context));
        }
      }
    }
    return new ScanBatch(context, readers.iterator());
  }
}
