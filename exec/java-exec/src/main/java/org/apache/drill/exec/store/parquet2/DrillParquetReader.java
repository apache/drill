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
import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.NullableBitVector;
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

  // same as the DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH in ParquetRecordReader

  private static final char DEFAULT_RECORDS_TO_READ = 32*1024;

  private ParquetMetadata footer;
  private MessageType schema;
  private DrillFileSystem fileSystem;
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

  // For columns not found in the file, we need to return a schema element with the correct number of values
  // at that position in the schema. Currently this requires a vector be present. Here is a list of all of these vectors
  // that need only have their value count set at the end of each call to next(), as the values default to null.
  private List<NullableIntVector> nullFilledVectors;
  // Keeps track of the number of records returned in the case where only columns outside of the file were selected.
  // No actual data needs to be read out of the file, we only need to return batches until we have 'read' the number of
  // records specified in the row group metadata
  long mockRecordsRead=0;
  private List<SchemaPath> columnsNotFound=null;
  boolean noColumnsFound = false; // true if none of the columns in the projection list is found in the schema


  public DrillParquetReader(FragmentContext fragmentContext, ParquetMetadata footer, RowGroupReadEntry entry,
      List<SchemaPath> columns, DrillFileSystem fileSystem) {
    this.footer = footer;
    this.fileSystem = fileSystem;
    this.entry = entry;
    setColumns(columns);
    this.fragmentContext = fragmentContext;
    fillLevelCheckFrequency = this.fragmentContext.getOptions().getOption(ExecConstants.PARQUET_VECTOR_FILL_CHECK_THRESHOLD).num_val.intValue();
    fillLevelCheckThreshold = this.fragmentContext.getOptions().getOption(ExecConstants.PARQUET_VECTOR_FILL_THRESHOLD).num_val.intValue();
  }

  public static MessageType getProjection(MessageType schema,
                                          Collection<SchemaPath> columns,
                                          List<SchemaPath> columnsNotFound) {
    MessageType projection = null;

    String messageName = schema.getName();
    List<ColumnDescriptor> schemaColumns = schema.getColumns();
    // parquet type.union() seems to lose ConvertedType info when merging two columns that are the same type. This can
    // happen when selecting two elements from an array. So to work around this, we use set of SchemaPath to avoid duplicates
    // and then merge the types at the end
    Set<SchemaPath> selectedSchemaPaths = Sets.newLinkedHashSet();

    // get a list of modified columns which have the array elements removed from the schema path since parquet schema doesn't include array elements
    List<SchemaPath> modifiedColumns = Lists.newLinkedList();
    for (SchemaPath path : columns) {
      List<String> segments = Lists.newArrayList();
      PathSegment seg = path.getRootSegment();
      do {
        if (seg.isNamed()) {
          segments.add(seg.getNameSegment().getPath());
        }
      } while ((seg = seg.getChild()) != null);
      String[] pathSegments = new String[segments.size()];
      segments.toArray(pathSegments);
      SchemaPath modifiedSchemaPath = SchemaPath.getCompoundPath(pathSegments);
      modifiedColumns.add(modifiedSchemaPath);
    }

    // convert the columns in the parquet schema to a list of SchemaPath columns so that they can be compared in case insensitive manner
    // to the projection columns
    List<SchemaPath> schemaPaths = Lists.newLinkedList();
    for (ColumnDescriptor columnDescriptor : schemaColumns) {
      String[] schemaColDesc = Arrays.copyOf(columnDescriptor.getPath(), columnDescriptor.getPath().length);
      SchemaPath schemaPath = SchemaPath.getCompoundPath(schemaColDesc);
      schemaPaths.add(schemaPath);
    }

    // loop through projection columns and add any columns that are missing from parquet schema to columnsNotFound list
    outer: for (SchemaPath columnPath : modifiedColumns) {
      boolean notFound = true;
      for (SchemaPath schemaPath : schemaPaths) {
        if (schemaPath.contains(columnPath)) {
          selectedSchemaPaths.add(schemaPath);
          notFound = false;
        }
      }
      if (notFound) {
        columnsNotFound.add(columnPath);
      }
    }

    // convert SchemaPaths from selectedSchemaPaths and convert to parquet type, and merge into projection schema
    for (SchemaPath schemaPath : selectedSchemaPaths) {
      List<String> segments = Lists.newArrayList();
      PathSegment seg = schemaPath.getRootSegment();
      do {
        segments.add(seg.getNameSegment().getPath());
      } while ((seg = seg.getChild()) != null);
      String[] pathSegments = new String[segments.size()];
      segments.toArray(pathSegments);
      Type t = getType(pathSegments, 0, schema);

      if (projection == null) {
        projection = new MessageType(messageName, t);
      } else {
        projection = projection.union(new MessageType(messageName, t));
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
        columnsNotFound=new ArrayList<SchemaPath>();
        projection = getProjection(schema, getColumns(), columnsNotFound);
        if(projection == null){
            projection = schema;
        }
        if(columnsNotFound!=null && columnsNotFound.size()>0) {
          nullFilledVectors = new ArrayList();
          for(SchemaPath col: columnsNotFound){
            nullFilledVectors.add(
              (NullableIntVector)output.addField(MaterializedField.create(col,
                  org.apache.drill.common.types.Types.optional(TypeProtos.MinorType.INT)),
                (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(TypeProtos.MinorType.INT,
                  TypeProtos.DataMode.OPTIONAL)));
          }
          if(columnsNotFound.size()==getColumns().size()){
            noColumnsFound=true;
          }
        }
      }

      logger.debug("Requesting schema {}", projection);

      ColumnIOFactory factory = new ColumnIOFactory(false);
      MessageColumnIO columnIO = factory.getColumnIO(projection, schema);
      Map<ColumnPath, ColumnChunkMetaData> paths = new HashMap();

      for (ColumnChunkMetaData md : footer.getBlocks().get(entry.getRowGroupIndex()).getColumns()) {
        paths.put(md.getPath(), md);
      }

      CodecFactoryExposer codecFactoryExposer = new CodecFactoryExposer(fileSystem.getConf());
      Path filePath = new Path(entry.getPath());

      BlockMetaData blockMetaData = footer.getBlocks().get(entry.getRowGroupIndex());

      recordCount = (int) blockMetaData.getRowCount();

      pageReadStore = new ColumnChunkIncReadStore(recordCount,
              codecFactoryExposer.getCodecFactory(), operatorContext.getAllocator(), fileSystem, filePath);

      for (String[] path : schema.getPaths()) {
        Type type = schema.getType(path);
        if (type.isPrimitive()) {
          ColumnChunkMetaData md = paths.get(ColumnPath.get(path));
          pageReadStore.addColumn(schema.getColumnDescription(path), md);
        }
      }

      if(!noColumnsFound) {
        writer = new VectorContainerWriter(output);
        recordMaterializer = new DrillParquetRecordMaterializer(output, writer, projection, getColumns());
        primitiveVectors = writer.getMapVector().getPrimitiveVectors();
        recordReader = columnIO.getRecordReader(pageReadStore, recordMaterializer);
      }
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

    // No columns found in the file were selected, simply return a full batch of null records for each column requested
    if (noColumnsFound) {
      if (mockRecordsRead == footer.getBlocks().get(entry.getRowGroupIndex()).getRowCount()) {
        return 0;
      }
      long recordsToRead = 0;
      recordsToRead = Math.min(DEFAULT_RECORDS_TO_READ, footer.getBlocks().get(entry.getRowGroupIndex()).getRowCount() - mockRecordsRead);
      for (ValueVector vv : nullFilledVectors ) {
        vv.getMutator().setValueCount( (int) recordsToRead);
      }
      mockRecordsRead += recordsToRead;
      return (int) recordsToRead;
    }

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
    // if we have requested columns that were not found in the file fill their vectors with null
    // (by simply setting the value counts inside of them, as they start null filled)
    if (nullFilledVectors != null) {
      for (ValueVector vv : nullFilledVectors ) {
        vv.getMutator().setValueCount(count);
      }
    }
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
