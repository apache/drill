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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class HBaseRecordReader implements RecordReader, DrillHBaseConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseRecordReader.class);

  private static final int TARGET_RECORD_COUNT = 4000;

  private List<SchemaPath> columns;
  private OutputMutator outputMutator;
  private Scan scan;
  private ResultScanner resultScanner;
  private FragmentContext context;
  Map<FamilyQualifierWrapper, NullableVarBinaryVector> vvMap;
  private Result leftOver;
  private VarBinaryVector rowKeyVector;
  private SchemaPath rowKeySchemaPath;
  private HTable table;

  public HBaseRecordReader(Configuration conf, HBaseSubScan.HBaseSubScanSpec e, List<SchemaPath> columns, FragmentContext context) {
    this.columns = columns;
    this.scan = new Scan(e.getStartRow(), e.getStopRow());
    this.scan.setFilter(e.getScanFilter());
    this.context = context;
    if (columns != null && columns.size() != 0) {
      for (SchemaPath column : columns) {
        if (column.getRootSegment().getPath().toString().equalsIgnoreCase(ROW_KEY)) {
          rowKeySchemaPath = ROW_KEY_PATH;
          continue;
        }
        NameSegment root = column.getRootSegment();
        assert root != null;
        PathSegment child = root.getChild();
        byte[] family = root.getPath().toString().getBytes();
        if (child != null) {
          Preconditions.checkArgument(child.getChild() == null, "Unsupported column name: " + column.toString());
          byte[] qualifier = child.getNameSegment().getPath().toString().getBytes();
          scan.addColumn(family, qualifier);
        } else {
          scan.addFamily(family);
        }

      }
    } else {
      if (this.columns == null) {
        this.columns = Lists.newArrayList();
      }
      rowKeySchemaPath = ROW_KEY_PATH;
      this.columns.add(rowKeySchemaPath);
    }

    Configuration config = HBaseConfiguration.create(conf);
    try {
      scan.setCaching(TARGET_RECORD_COUNT);
      table = new HTable(config, e.getTableName());
      resultScanner = table.getScanner(scan);
    } catch (IOException e1) {
      throw new DrillRuntimeException(e1);
    }
  }

  @Override
  @SuppressWarnings("deprecation")
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    output.removeAllFields();
    vvMap = new HashMap<FamilyQualifierWrapper, NullableVarBinaryVector>();

    // Add Vectors to output in the order specified when creating reader
    for (SchemaPath column : columns) {
      try {
        if (column.equals(rowKeySchemaPath)) {
          MaterializedField field = MaterializedField.create(column, Types.required(TypeProtos.MinorType.VARBINARY));
          rowKeyVector = new VarBinaryVector(field, context.getAllocator());
          output.addField(rowKeyVector);
        } else if (column.getRootSegment().getChild() != null){
          MaterializedField field = MaterializedField.create(column, Types.optional(TypeProtos.MinorType.VARBINARY));
          NullableVarBinaryVector v = new NullableVarBinaryVector(field, context.getAllocator());
          output.addField(v);
          String fullyQualified = column.getRootSegment().getPath() + "." + column.getRootSegment().getChild().getNameSegment().getPath();
          vvMap.put(new FamilyQualifierWrapper(fullyQualified), v);
        }
      } catch (SchemaChangeException e) {
        throw new ExecutionSetupException(e);
      }
    }

    try {
      output.setNewSchema();
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = new Stopwatch();
    watch.start();
    if (rowKeyVector != null) {
      rowKeyVector.clear();
      VectorAllocator.getAllocator(rowKeyVector, 100).alloc(TARGET_RECORD_COUNT);
    }
    for (ValueVector v : vvMap.values()) {
      v.clear();
      VectorAllocator.getAllocator(v, 100).alloc(TARGET_RECORD_COUNT);
    }
    for (int count = 0; count < TARGET_RECORD_COUNT; count++) {
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
        setOutputValueCount(count);
        logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), count);
        return count;
      }
      KeyValue[] kvs = result.raw();
      byte[] bytes = result.getBytes().get();
      if (rowKeyVector != null) {
        if (!rowKeyVector.getMutator().setSafe(count, bytes, kvs[0].getRowOffset(), kvs[0].getRowLength())) {
          setOutputValueCount(count);
          leftOver = result;
          logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), count);
          return count;
        }
      }
      for (KeyValue kv : kvs) {
        int familyOffset = kv.getFamilyOffset();
        int familyLength = kv.getFamilyLength();
        int qualifierOffset = kv.getQualifierOffset();
        int qualifierLength = kv.getQualifierLength();
        FamilyQualifierWrapper column = new FamilyQualifierWrapper(bytes, familyOffset, familyLength, qualifierOffset, qualifierLength);
        NullableVarBinaryVector v = vvMap.get(column);
        if(v == null) {
          v = addNewVector(column.toString());
        }
        int valueOffset = kv.getValueOffset();
        int valueLength = kv.getValueLength();
        if (!v.getMutator().setSafe(count, bytes, valueOffset, valueLength)) {
          setOutputValueCount(count);
          leftOver = result;
          logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), count);
          return count;
        }
      }
    }
    setOutputValueCount(TARGET_RECORD_COUNT);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), TARGET_RECORD_COUNT);
    return TARGET_RECORD_COUNT;
  }

  @SuppressWarnings("deprecation")
  private NullableVarBinaryVector addNewVector(String column) {
    MaterializedField field = MaterializedField.create(SchemaPath.getCompoundPath(column.split("\\.")), Types.optional(TypeProtos.MinorType.VARBINARY));
    NullableVarBinaryVector v = new NullableVarBinaryVector(field, context.getAllocator());
    VectorAllocator.getAllocator(v, 100).alloc(TARGET_RECORD_COUNT);
    vvMap.put(new FamilyQualifierWrapper(column), v);
    try {
      outputMutator.addField(v);
      outputMutator.setNewSchema();
    } catch (SchemaChangeException e) {
      throw new DrillRuntimeException(e);
    }
    return v;
  }

  @Override
  public void cleanup() {
    resultScanner.close();
    try {
      table.close();
    } catch (IOException e) {
      logger.warn("Failure while closing table", e);
    }
  }

  private void setOutputValueCount(int count) {
    for (ValueVector vv : vvMap.values()) {
      vv.getMutator().setValueCount(count);
    }
    if (rowKeyVector != null) {
      rowKeyVector.getMutator().setValueCount(count);
    }
  }

  private static int compareArrays(byte[] left, int lstart, int llength, byte[] right, int rstart, int rlength) {
    int length = Math.min(llength, rlength);
    for (int i = 0; i < length; i++) {
      if (left[lstart + i] != right[rstart + i]) {
        return left[lstart + i] - right[rstart + 1];
      }
    }
    return llength - rlength;
  }

  private static class FamilyQualifierWrapper implements Comparable<FamilyQualifierWrapper> {
    static final HashFunction hashFunction = Hashing.murmur3_32();

    protected byte[] bytes;
    protected int familyOffset, familyLength, qualifierOffset, qualifierLength;
    String string;
    int hashCode;

    public FamilyQualifierWrapper(byte[] bytes, int familyOffset, int familyLength, int qualifierOffset, int qualifierLength) {
      this.bytes = bytes;
      this.familyOffset = familyOffset;
      this.familyLength = familyLength;
      this.qualifierOffset = qualifierOffset;
      this.qualifierLength = qualifierLength;
      Hasher hasher = hashFunction.newHasher();
      hasher.putBytes(bytes, familyOffset, familyLength);
      hasher.putBytes(bytes, qualifierOffset, qualifierLength);
      hashCode = hasher.hash().asInt();
    }

    public FamilyQualifierWrapper(String string) {
      String[] strings = string.split("\\.");
      this.string = string;
      Hasher hasher = hashFunction.newHasher();
      byte[] fBytes = strings[0].getBytes();
      byte[] qBytes = strings[1].getBytes();
      hasher.putBytes(fBytes);
      hasher.putBytes(qBytes);
      familyLength = fBytes.length;
      qualifierLength = qBytes.length;
      hashCode = hasher.hash().asInt();
    }

    @Override
    public int hashCode() {
      return this.hashCode;
    }

    @Override
    public boolean equals(Object other) {
      return compareTo((FamilyQualifierWrapper) other) == 0;
    }

    @Override
    public String toString() {
      if (string == null) {
        buildString();
      }
      return string;
    }

    public void buildString() {
      StringBuilder builder = new StringBuilder();
      builder.append(new String(bytes, familyOffset, familyLength));
      builder.append(".");
      builder.append(new String(bytes, qualifierOffset, qualifierLength));
      string = builder.toString();
    }

    public void buildBytes() {
      assert string != null;
      bytes = string.getBytes();
      familyOffset = 0;
      qualifierOffset = familyLength + 1;
    }

    @Override
    public int compareTo(FamilyQualifierWrapper o) {
      if (bytes == null) {
        buildBytes();
      }
      if (o.bytes == null) {
        o.buildBytes();
      }
      int val = Bytes.compareTo(bytes, familyOffset, familyLength, o.bytes, o.familyOffset, o.familyLength);
      if (val != 0) {
        return val;
      }
      return Bytes.compareTo(bytes, qualifierOffset, qualifierLength, o.bytes, o.qualifierOffset, o.qualifierLength);
    }
  }
}
