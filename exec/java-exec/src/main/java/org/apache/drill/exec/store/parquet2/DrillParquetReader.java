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
package org.apache.drill.exec.store.parquet2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.parquet.ParquetDirectByteBufferAllocator;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ColumnChunkIncReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;

public class DrillParquetReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillParquetReader.class);

  // same as the DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH in ParquetRecordReader

  private static final char DEFAULT_RECORDS_TO_READ = 32*1024;

  private ParquetMetadata footer;
  private MessageType schema;
  private DrillFileSystem fileSystem;
  private RowGroupReadEntry entry;
  private ColumnChunkIncReadStore pageReadStore;
  private RecordReader<Void> recordReader;
  private DrillParquetRecordMaterializer recordMaterializer;
  private int recordCount;
  private OperatorContext operatorContext;
  private FragmentContext fragmentContext;
  /** Configured Parquet records per batch */
  private final int recordsPerBatch;

  // For columns not found in the file, we need to return a schema element with the correct number of values
  // at that position in the schema. Currently this requires a vector be present. Here is a list of all of these vectors
  // that need only have their value count set at the end of each call to next(), as the values default to null.
  private List<NullableIntVector> nullFilledVectors;
  // Keeps track of the number of records returned in the case where only columns outside of the file were selected.
  // No actual data needs to be read out of the file, we only need to return batches until we have 'read' the number of
  // records specified in the row group metadata
  long mockRecordsRead=0;
  private List<SchemaPath> columnsNotFound;
  boolean noColumnsFound; // true if none of the columns in the projection list is found in the schema

  // See DRILL-4203
  private final ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates;

  public DrillParquetReader(FragmentContext fragmentContext, ParquetMetadata footer, RowGroupReadEntry entry,
      List<SchemaPath> columns, DrillFileSystem fileSystem, ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates) {
    this.containsCorruptedDates = containsCorruptedDates;
    this.footer = footer;
    this.fileSystem = fileSystem;
    this.entry = entry;
    setColumns(columns);
    this.fragmentContext = fragmentContext;
    this.recordsPerBatch = (int) fragmentContext.getOptions().getLong(ExecConstants.PARQUET_COMPLEX_BATCH_NUM_RECORDS);
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
    for (SchemaPath columnPath : modifiedColumns) {
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
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    try {
      for (final ValueVector v : vectorMap.values()) {
        AllocationHelper.allocate(v, Character.MAX_VALUE, 50, 10);
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {

    try {
      this.operatorContext = context;
      schema = footer.getFileMetaData().getSchema();
      MessageType projection;

      if (isStarQuery()) {
        projection = schema;
      } else {
        columnsNotFound = new ArrayList<>();
        projection = getProjection(schema, getColumns(), columnsNotFound);
        if (projection == null) {
            projection = schema;
        }
        if (columnsNotFound != null && columnsNotFound.size() > 0) {
          nullFilledVectors = new ArrayList<>();
          for (SchemaPath col: columnsNotFound) {
            // col.toExpr() is used here as field name since we don't want to see these fields in the existing maps
            nullFilledVectors.add(
              (NullableIntVector) output.addField(MaterializedField.create(col.toExpr(),
                  org.apache.drill.common.types.Types.optional(TypeProtos.MinorType.INT)),
                (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(TypeProtos.MinorType.INT,
                  TypeProtos.DataMode.OPTIONAL)));
          }
          if (columnsNotFound.size() == getColumns().size()) {
            noColumnsFound = true;
          }
        }
      }

      logger.debug("Requesting schema {}", projection);

      ColumnIOFactory factory = new ColumnIOFactory(false);
      MessageColumnIO columnIO = factory.getColumnIO(projection, schema);
      Map<ColumnPath, ColumnChunkMetaData> paths = new HashMap<>();

      for (ColumnChunkMetaData md : footer.getBlocks().get(entry.getRowGroupIndex()).getColumns()) {
        paths.put(md.getPath(), md);
      }

      Path filePath = new Path(entry.getPath());

      BlockMetaData blockMetaData = footer.getBlocks().get(entry.getRowGroupIndex());

      recordCount = (int) blockMetaData.getRowCount();

      pageReadStore = new ColumnChunkIncReadStore(recordCount,
          CodecFactory.createDirectCodecFactory(fileSystem.getConf(),
              new ParquetDirectByteBufferAllocator(operatorContext.getAllocator()), 0), operatorContext.getAllocator(),
          fileSystem, filePath);

      for (String[] path : schema.getPaths()) {
        Type type = schema.getType(path);
        if (type.isPrimitive()) {
          ColumnChunkMetaData md = paths.get(ColumnPath.get(path));
          pageReadStore.addColumn(schema.getColumnDescription(path), md);
        }
      }

      if (!noColumnsFound) {
        // Discard the columns not found in the schema when create DrillParquetRecordMaterializer, since they have been added to output already.
        @SuppressWarnings("unchecked")
        final Collection<SchemaPath> columns = columnsNotFound == null || columnsNotFound.size() == 0 ? getColumns(): CollectionUtils.subtract(getColumns(), columnsNotFound);
        recordMaterializer = new DrillParquetRecordMaterializer(output, projection, columns, fragmentContext.getOptions(), containsCorruptedDates);
        recordReader = columnIO.getRecordReader(pageReadStore, recordMaterializer);
      }
    } catch (Exception e) {
      handleAndRaise("Failure in setting up reader", e);
    }
  }

  protected void handleAndRaise(String s, Exception e) {
    close();
    String message = "Error in drill parquet reader (complex).\nMessage: " + s +
      "\nParquet Metadata: " + footer;
    throw new DrillRuntimeException(message, e);
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

    while (count < recordsPerBatch && totalRead < recordCount) {
      recordMaterializer.setPosition(count);
      recordReader.read();
      count++;
      totalRead++;
    }
    recordMaterializer.setValueCount(count);
    // if we have requested columns that were not found in the file fill their vectors with null
    // (by simply setting the value counts inside of them, as they start null filled)
    if (nullFilledVectors != null) {
      for (final ValueVector vv : nullFilledVectors) {
        vv.getMutator().setValueCount(count);
      }
    }
    return count;
  }

  @Override
  public void close() {
    footer = null;
    fileSystem = null;
    entry = null;
    recordReader = null;
    recordMaterializer = null;
    nullFilledVectors = null;
    columnsNotFound = null;
    try {
      if (pageReadStore != null) {
        pageReadStore.close();
        pageReadStore = null;
      }
    } catch (IOException e) {
      logger.warn("Failure while closing PageReadStore", e);
    }
  }

  static public class ProjectedColumnType {
    public final String projectedColumnName;
    public final MessageType type;

    ProjectedColumnType(String projectedColumnName, MessageType type) {
      this.projectedColumnName = projectedColumnName;
      this.type = type;
    }
  }

  @Override
  public String toString() {
    return "DrillParquetReader[pageReadStore=" + pageReadStore + "]";
  }
}
