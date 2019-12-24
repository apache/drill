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

package org.apache.drill.exec.store.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ElasticSearchBatchCreator implements BatchCreator<ElasticSearchSubScan> {

  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchBatchCreator.class);

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, ElasticSearchSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    List<RecordReader> readers = Lists.newArrayList();
    List<SchemaPath> columns = null;
    if ((columns = subScan.getColumns()) == null) {
      columns = GroupScan.ALL_COLUMNS;
    }
    try {
      for (ElasticSearchScanSpec spec : subScan.getElasticSearchScanSpecs()) {
        // 这里应该批量读取数据的
        readers.add(new ElasticSearchRecordReader(spec, columns, context, subScan.getElasticSearchStoragePlugin()));
      }
    } catch (Exception e) {
      logger.error("ElasticSearchRecordReader creation failed for subScan:  " + subScan + ".");
      logger.error(e.getMessage(), e);
      throw new ExecutionSetupException(e);
    }
    logger.info("Number of record readers initialized : " + readers.size());
    return new ScanBatch(subScan, context, readers);
  }
}
