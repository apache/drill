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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.column.ColumnDescriptor;
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
import parquet.schema.PrimitiveType;


import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import parquet.schema.Types;

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
  // The interface for the parquet-mr library does not allow re-winding, to enable us to write into our
  // fixed size value vectors, we must check how full the vectors are after some number of reads, for performance
  // we avoid doing this every record. These values are populated with system/session settings to allow users to optimize
  // for performance or allow a wider record size to be suported
  private final int fillLevelCheckFrequency;
  private final int fillLevelCheckThreshold;
  private FragmentContext fragmentContext;


  public DrillParquetReader(FragmentContext fragmentContext, ParquetMetadata footer, RowGroupReadEntry entry, List<SchemaPath> columns, Configuration conf) {
    this.footer = footer;
    this.conf = conf;
    this.entry = entry;
    setColumns(columns);
    this.fragmentContext = fragmentContext;
    fillLevelCheckFrequency = this.fragmentContext.getOptions().getOption(ExecConstants.PARQUET_VECTOR_FILL_CHECK_THRESHOLD).num_val.intValue();
    fillLevelCheckThreshold = this.fragmentContext.getOptions().getOption(ExecConstants.PARQUET_VECTOR_FILL_THRESHOLD).num_val.intValue();
  }

  public static MessageType getProjection(MessageType schema, Collection<SchemaPath> columns) {
    MessageType projection = null;

    String messageName = schema.getName();
    List<ColumnDescriptor> schemaColumns = schema.getColumns();

    for (SchemaPath path : columns) {
      for (ColumnDescriptor colDesc: schemaColumns) {
        String[] schemaColDesc = colDesc.getPath();
        SchemaPath schemaPath = SchemaPath.getCompoundPath(schemaColDesc);

        PathSegment schemaSeg = schemaPath.getRootSegment();
        PathSegment colSeg = path.getRootSegment();
        List<String> segments = Lists.newArrayList();
        List<String> colSegments = Lists.newArrayList();
        while(schemaSeg != null && colSeg != null){
          if (colSeg.isNamed()) {
            // DRILL-1739 - Use case insensitive name comparison
            if(schemaSeg.getNameSegment().getPath().equalsIgnoreCase(colSeg.getNameSegment().getPath())) {
              segments.add(schemaSeg.getNameSegment().getPath());
              colSegments.add(colSeg.getNameSegment().getPath());
            }else{
              break;
            }
          }else{
            colSeg=colSeg.getChild();
            continue;
          }
          colSeg = colSeg.getChild();
          schemaSeg = schemaSeg.getChild();
        }
        // Field exists in schema
        if (!segments.isEmpty()) {
          String[] pathSegments = new String[segments.size()];
          segments.toArray(pathSegments);
          String[] colPathSegments = new String[colSegments.size()];
          colSegments.toArray(colPathSegments);

          // Use the field names from the schema or we get an exception if the case of the name doesn't match
          Type t = getType(colPathSegments, pathSegments, 0, schema);

          if (projection == null) {
            projection = new MessageType(messageName, t);
          } else {
            projection = projection.union(new MessageType(messageName, t));
          }
          break;
        }
      }
    }
    return projection;
  }

  @Override
  public void allocate(Map<Key, ValueVector> vectorMap) throws OutOfMemoryException {
    try {
      for (ValueVector v : vectorMap.values()) {
        AllocationHelper.allocate(v, Character.MAX_VALUE, 50, 10);
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
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
      recordMaterializer = new DrillParquetRecordMaterializer(output, writer, projection, getColumns());
      primitiveVectors = writer.getMapVector().getPrimitiveVectors();
      recordReader = columnIO.getRecordReader(pageReadStore, recordMaterializer);
    } catch (Exception e) {
      throw new ExecutionSetupException(e);
    }
  }

  private static Type getType(String[] colSegs, String[] pathSegments, int depth, MessageType schema) {
    Type type = schema.getType(Arrays.copyOfRange(pathSegments, 0, depth + 1));
    //String name = colSegs[depth]; // get the name from the column list not the schema
    if (depth + 1 == pathSegments.length) {
      //type.name = colSegs[depth];
      //Type newType = type;

      //if(type.isPrimitive()){
      //  //newType = new PrimitiveType(type.getRepetition(), type.asPrimitiveType().getPrimitiveTypeName(), name, type.getOriginalType());
      //  Types.PrimitiveBuilder<PrimitiveType> builder = Types.primitive(
      //    type.asPrimitiveType().getPrimitiveTypeName(), type.getRepetition());
      //  if (PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY == type.asPrimitiveType().getPrimitiveTypeName()) {
      //    builder.length(type.asPrimitiveType().getTypeLength());
      //  }
      //  newType = builder.named(name);
      //}else{
      //  //newType = new GroupType(type.getRepetition(), name, type);
      //  newType = new GroupType(type.getRepetition(), name, type.asGroupType().getFields());
      //}
      return type;
    } else {
      Preconditions.checkState(!type.isPrimitive());
      return new GroupType(type.getRepetition(), type.getName(), getType(colSegs, pathSegments, depth + 1, schema));
    }
  }

  private long totalRead = 0;

  @Override
  public int next() {
    int count = 0;
    while (count < 4000 && totalRead < recordCount) {
      recordMaterializer.setPosition(count);
      recordReader.read();
      count++;
      totalRead++;
      if (count % fillLevelCheckFrequency == 0) {
        if (getPercentFilled() > fillLevelCheckThreshold) {
          if(!recordMaterializer.ok()){
            throw new RuntimeException(String.format("The setting for `%s` is too high for your Parquet records. Please set a lower check threshold and retry your query. ", ExecConstants.PARQUET_VECTOR_FILL_CHECK_THRESHOLD));
          }
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
      // TODO - need to re-enable this
//      if (v instanceof RepeatedFixedWidthVector) {
//        filled = Math.max(filled, ((RepeatedFixedWidthVector) v).getAccessor().getGroupCount() * 100)
//      }
    }
    logger.debug("Percent filled: {}", filled);
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

  static public class ProjectedColumnType{
    ProjectedColumnType(String projectedColumnName, MessageType type){
      this.projectedColumnName=projectedColumnName;
      this.type=type;
    }

    public String projectedColumnName;
    public MessageType type;


  }

}
