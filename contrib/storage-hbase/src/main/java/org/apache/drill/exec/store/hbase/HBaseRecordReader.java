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
package org.apache.drill.exec.store.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

public class HBaseRecordReader extends AbstractRecordReader implements DrillHBaseConstants {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseRecordReader.class);

  private static final int TARGET_RECORD_COUNT = 4000;

  private OutputMutator outputMutator;

  private Map<String, MapVector> familyVectorMap;
  private VarBinaryVector rowKeyVector;

  private HTable hTable;
  private ResultScanner resultScanner;

  private String hbaseTableName;
  private Scan hbaseScan;
  private Configuration hbaseConf;
  private Result leftOver;
  private FragmentContext fragmentContext;
  private OperatorContext operatorContext;


  public HBaseRecordReader(Configuration conf, HBaseSubScan.HBaseSubScanSpec subScanSpec,
      List<SchemaPath> projectedColumns, FragmentContext context) throws OutOfMemoryException {
    hbaseConf = conf;
    hbaseTableName = Preconditions.checkNotNull(subScanSpec, "HBase reader needs a sub-scan spec").getTableName();
    hbaseScan = new Scan(subScanSpec.getStartRow(), subScanSpec.getStopRow());
    hbaseScan
        .setFilter(subScanSpec.getScanFilter())
        .setCaching(TARGET_RECORD_COUNT);

    setColumns(projectedColumns);
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      boolean rowKeyOnly = true;
      for (SchemaPath column : columns) {
        if (column.getRootSegment().getPath().equalsIgnoreCase(ROW_KEY)) {
          transformed.add(ROW_KEY_PATH);
          continue;
        }
        rowKeyOnly = false;
        NameSegment root = column.getRootSegment();
        byte[] family = root.getPath().getBytes();
        transformed.add(SchemaPath.getSimplePath(root.getPath()));
        PathSegment child = root.getChild();
        if (child != null && child.isNamed()) {
          byte[] qualifier = child.getNameSegment().getPath().getBytes();
          hbaseScan.addColumn(family, qualifier);
        } else {
          hbaseScan.addFamily(family);
        }
      }
      /* if only the row key was requested, add a FirstKeyOnlyFilter to the scan
       * to fetch only one KV from each row. If a filter is already part of this
       * scan, add the FirstKeyOnlyFilter as the LAST filter of a MUST_PASS_ALL
       * FilterList.
       */
      if (rowKeyOnly) {
        hbaseScan.setFilter(
            HBaseUtils.andFilterAtIndex(hbaseScan.getFilter(), HBaseUtils.LAST_FILTER, new FirstKeyOnlyFilter()));
      }
    } else {
      transformed.add(ROW_KEY_PATH);
    }


    return transformed;
  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }


  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    familyVectorMap = new HashMap<String, MapVector>();

    try {
      // Add Vectors to output in the order specified when creating reader
      for (SchemaPath column : getColumns()) {
        if (column.equals(ROW_KEY_PATH)) {
          MaterializedField field = MaterializedField.create(column, ROW_KEY_TYPE);
          rowKeyVector = outputMutator.addField(field, VarBinaryVector.class);
        } else {
          getOrCreateFamilyVector(column.getRootSegment().getPath(), false);
        }
      }
      logger.debug("Opening scanner for HBase table '{}', Zookeeper quorum '{}', port '{}', znode '{}'.",
          hbaseTableName, hbaseConf.get(HConstants.ZOOKEEPER_QUORUM),
          hbaseConf.get(HBASE_ZOOKEEPER_PORT), hbaseConf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
      hTable = new HTable(hbaseConf, hbaseTableName);
      resultScanner = hTable.getScanner(hbaseScan);
    } catch (SchemaChangeException | IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = new Stopwatch();
    watch.start();
    if (rowKeyVector != null) {
      rowKeyVector.clear();
      rowKeyVector.allocateNew();
    }
    for (ValueVector v : familyVectorMap.values()) {
      v.clear();
      v.allocateNew();
    }

    int rowCount = 0;
    done:
    for (; rowCount < TARGET_RECORD_COUNT; rowCount++) {
      Result result = null;
      try {
        if (leftOver != null) {
          result = leftOver;
          leftOver = null;
        } else {
          result = resultScanner.next();
        }
      } catch (IOException e) {
        throw new DrillRuntimeException(e);
      }
      if (result == null) {
        break done;
      }

      // parse the result and populate the value vectors
      KeyValue[] kvs = result.raw();
      byte[] bytes = result.getBytes().get();
      if (rowKeyVector != null) {
        if (!rowKeyVector.getMutator().setSafe(rowCount, bytes, kvs[0].getRowOffset(), kvs[0].getRowLength())) {
          leftOver = result;
          break done;
        }
      }

      for (KeyValue kv : kvs) {
        int familyOffset = kv.getFamilyOffset();
        int familyLength = kv.getFamilyLength();
        MapVector mv = getOrCreateFamilyVector(new String(bytes, familyOffset, familyLength), true);

        int qualifierOffset = kv.getQualifierOffset();
        int qualifierLength = kv.getQualifierLength();
        NullableVarBinaryVector v = getOrCreateColumnVector(mv, new String(bytes, qualifierOffset, qualifierLength));

        int valueOffset = kv.getValueOffset();
        int valueLength = kv.getValueLength();
        if (!v.getMutator().setSafe(rowCount, bytes, valueOffset, valueLength)) {
          leftOver = result;
          return rowCount;
        }
      }
    }

    setOutputRowCount(rowCount);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), rowCount);
    return rowCount;
  }

  private MapVector getOrCreateFamilyVector(String familyName, boolean allocateOnCreate) {
    try {
      MapVector v = familyVectorMap.get(familyName);
      if(v == null) {
        SchemaPath column = SchemaPath.getSimplePath(familyName);
        MaterializedField field = MaterializedField.create(column, COLUMN_FAMILY_TYPE);
        v = outputMutator.addField(field, MapVector.class);
        if (allocateOnCreate) {
          v.allocateNew();
        }
        getColumns().add(column);
        familyVectorMap.put(familyName, v);
      }
      return v;
    } catch (SchemaChangeException e) {
      throw new DrillRuntimeException(e);
    }
  }

  private NullableVarBinaryVector getOrCreateColumnVector(MapVector mv, String qualifier) {
    int oldSize = mv.size();
    NullableVarBinaryVector v = mv.addOrGet(qualifier, COLUMN_TYPE, NullableVarBinaryVector.class);
    if (oldSize != mv.size()) {
      v.allocateNew();
    }
    return v;
  }

  @Override
  public void cleanup() {
    try {
      if (resultScanner != null) {
        resultScanner.close();
      }
      if (hTable != null) {
        hTable.close();
      }
    } catch (IOException e) {
      logger.warn("Failure while closing HBase table: " + hbaseTableName, e);
    }
  }

  private void setOutputRowCount(int count) {
    for (ValueVector vv : familyVectorMap.values()) {
      vv.getMutator().setValueCount(count);
    }
    if (rowKeyVector != null) {
      rowKeyVector.getMutator().setValueCount(count);
    }
  }

}
