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
package org.apache.drill.exec.store.parquet2;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.ColumnChunkIncReadStore;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.ColumnIOFactory;
import parquet.io.InvalidRecordException;
import parquet.io.MessageColumnIO;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DrillParquetReader extends AbstractRecordReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillParquetReader.class);

  private ParquetMetadata footer;
  private MessageType schema;
  private Configuration conf;
  private RowGroupReadEntry entry;
  private VectorContainerWriter writer;
  private ColumnChunkIncReadStore pageReadStore;
  private parquet.io.RecordReader<Void> recordReader;
  private DrillParquetRecordMaterializer recordMaterializer;
  private int recordCount;
  private List<ValueVector> primitiveVectors;
  private OperatorContext operatorContext;


  public DrillParquetReader(ParquetMetadata footer, RowGroupReadEntry entry, List<SchemaPath> columns, Configuration conf) {
    this.footer = footer;
    this.conf = conf;
    this.entry = entry;
    setColumns(columns);
  }

  public static MessageType getProjection(MessageType schema, Collection<SchemaPath> columns) {
    MessageType projection = null;
    for (SchemaPath path : columns) {
      List<String> segments = Lists.newArrayList();
      PathSegment rootSegment = path.getRootSegment();
      PathSegment seg = rootSegment;
      String messageName = schema.getName();
      while(seg != null){
        if(seg.isNamed()) {
          segments.add(seg.getNameSegment().getPath());
        }
        seg = seg.getChild();
      }
      String[] pathSegments = new String[segments.size()];
      segments.toArray(pathSegments);
      Type type = null;
      try {
        type = schema.getType(pathSegments);
      } catch (InvalidRecordException e) {
        logger.warn("Invalid record" , e);
      }
      if (type != null) {
        Type t = getType(pathSegments, 0, schema);
        if (projection == null) {
          projection = new MessageType(messageName, t);
        } else {
          projection = projection.union(new MessageType(messageName, t));
        }
      }
    }
    return projection;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {

    try {
      schema = footer.getFileMetaData().getSchema();
      MessageType projection = null;

      if (isStarQuery()) {
        projection = schema;
      } else {
        projection = getProjection(schema, getColumns());
        if (projection == null) {
          projection = schema;
        }
      }

      logger.debug("Requesting schema {}", projection);

      ColumnIOFactory factory = new ColumnIOFactory(false);
      MessageColumnIO columnIO = factory.getColumnIO(projection, schema);
      Map<ColumnPath, ColumnChunkMetaData> paths = new HashMap();

      for (ColumnChunkMetaData md : footer.getBlocks().get(entry.getRowGroupIndex()).getColumns()) {
        paths.put(md.getPath(), md);
      }

      CodecFactoryExposer codecFactoryExposer = new CodecFactoryExposer(conf);
      FileSystem fs = FileSystem.get(conf);
      Path filePath = new Path(entry.getPath());

      BlockMetaData blockMetaData = footer.getBlocks().get(entry.getRowGroupIndex());

      recordCount = (int) blockMetaData.getRowCount();

      pageReadStore = new ColumnChunkIncReadStore(recordCount,
              codecFactoryExposer.getCodecFactory(), operatorContext.getAllocator(), fs, filePath);

      for (String[] path : schema.getPaths()) {
        Type type = schema.getType(path);
        if (type.isPrimitive()) {
          ColumnChunkMetaData md = paths.get(ColumnPath.get(path));
          pageReadStore.addColumn(schema.getColumnDescription(path), md);
        }
      }

      writer = new VectorContainerWriter(output);
      recordMaterializer = new DrillParquetRecordMaterializer(output, writer, projection);
      primitiveVectors = writer.getMapVector().getPrimitiveVectors();
      recordReader = columnIO.getRecordReader(pageReadStore, recordMaterializer);
    } catch (Exception e) {
      throw new ExecutionSetupException(e);
    }
  }

  private static Type getType(String[] pathSegments, int depth, MessageType schema) {
    Type type = schema.getType(Arrays.copyOfRange(pathSegments, 0, depth + 1));
    if (depth + 1 == pathSegments.length) {
      return type;
    } else {
      Preconditions.checkState(!type.isPrimitive());
      return new GroupType(type.getRepetition(), type.getName(), getType(pathSegments, depth + 1, schema));
    }
  }

  private long totalRead = 0;

  @Override
  public int next() {
    int count = 0;
    for (; count < 4000 && totalRead < recordCount; count++, totalRead++) {
      recordMaterializer.setPosition(count);
      recordReader.read();
      if (count % 100 == 0) {
        if (getPercentFilled() > 85) {
          break;
        }
      }
    }
    writer.setValueCount(count);
    return count;
  }

  private int getPercentFilled() {
    int filled = 0;
    for (ValueVector v : primitiveVectors) {
      filled = Math.max(filled, ((BaseValueVector) v).getCurrentValueCount() * 100 / v.getValueCapacity());
      if (v instanceof VariableWidthVector) {
        filled = Math.max(filled, ((VariableWidthVector) v).getCurrentSizeInBytes() * 100 / ((VariableWidthVector) v).getByteCapacity());
      }
    }
    return filled;
  }

  @Override
  public void cleanup() {
    try {
      pageReadStore.close();
    } catch (IOException e) {
      logger.warn("Failure while closing PageReadStore", e);
    }
  }

  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }


}
