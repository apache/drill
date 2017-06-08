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

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.InputSplit;

import com.google.common.collect.Lists;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;

@SuppressWarnings("unused")
public class HiveScanBatchCreator implements BatchCreator<HiveSubScan> {

  /**
   * Use different classes for different Hive native formats:
   * ORC, AVRO, RCFFile, Text and Parquet.
   * If input format is none of them falls to default reader.
   */
  static Map<String, Class> readerMap = new HashMap<>();
  static {
    readerMap.put(OrcInputFormat.class.getCanonicalName(), HiveOrcReader.class);
    readerMap.put(AvroContainerInputFormat.class.getCanonicalName(), HiveAvroReader.class);
    readerMap.put(RCFileInputFormat.class.getCanonicalName(), HiveRCFileReader.class);
    readerMap.put(MapredParquetInputFormat.class.getCanonicalName(), HiveParquetReader.class);
    readerMap.put(TextInputFormat.class.getCanonicalName(), HiveTextReader.class);
  }

  @Override
  public ScanBatch getBatch(FragmentContext context, HiveSubScan config, List<RecordBatch> children)
      throws ExecutionSetupException {
    List<RecordReader> readers = Lists.newArrayList();
    HiveTableWithColumnCache table = config.getTable();
    List<InputSplit> splits = config.getInputSplits();
    List<HivePartition> partitions = config.getPartitions();
    boolean hasPartitions = (partitions != null && partitions.size() > 0);
    int i = 0;
    final UserGroupInformation proxyUgi = ImpersonationUtil.createProxyUgi(config.getUserName(),
      context.getQueryUserName());

    final HiveConf hiveConf = config.getHiveConf();

    final String formatName = table.getSd().getInputFormat();
    Class<? extends HiveAbstractReader> readerClass = HiveDefaultReader.class;
    if (readerMap.containsKey(formatName)) {
      readerClass = readerMap.get(formatName);
    }
    Constructor<? extends HiveAbstractReader> readerConstructor = null;
    try {
      readerConstructor = readerClass.getConstructor(HiveTableWithColumnCache.class, HivePartition.class,
          InputSplit.class, List.class, FragmentContext.class, HiveConf.class,
          UserGroupInformation.class);
      for (InputSplit split : splits) {
        readers.add(readerConstructor.newInstance(table,
            (hasPartitions ? partitions.get(i++) : null), split, config.getColumns(), context, hiveConf, proxyUgi));
      }
      if (readers.size() == 0) {
        readers.add(readerConstructor.newInstance(
            table, null, null, config.getColumns(), context, hiveConf, proxyUgi));
      }
    } catch(Exception e) {
      logger.error("No constructor for {}, thrown {}", readerClass.getName(), e);
    }
    return new ScanBatch(config, context, readers.iterator());
  }
}
