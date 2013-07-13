/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.store.orc;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Sets;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.VectorHolder;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class OrcRecordReader implements org.apache.drill.exec.store.RecordReader {
  private final FileSystem fileSystem;
  private final Path path;
  private VectorizedRowBatch batch;
  private BufferAllocator allocator;
  private RecordReader orcReader;
  private OutputMutator mutator;
  private List<OrcColumnReader> holders;
  private long batchSize;

  public OrcRecordReader(FragmentContext context, FileSystem fileSystem, Path path, long batchSize) {
    this.allocator = context.getAllocator();
    this.fileSystem = fileSystem;
    this.path = path;
    this.holders = Lists.newArrayList();
    checkArgument(batchSize > 0);
    this.batchSize = batchSize;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      Reader reader = OrcFile.createReader(fileSystem, path);
      this.orcReader = reader.rows(null); // skip predicate push down for now
      this.batch = null;
      holders.clear();
      mutator = output;
      setupColumns(output, reader.getTypes());
    } catch (IOException | SchemaChangeException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new ExecutionSetupException(e);
    }
  }

  private void setupColumns(OutputMutator output, List<OrcProto.Type> types) throws SchemaChangeException, IllegalAccessException, InvocationTargetException, InstantiationException {
    output.removeAllFields();
    populateStructColumns(types, types.get(0));
  }

  private static OrcColumnReader createOrcColumnReader(String fieldName, BufferAllocator allocator, OrcProto.Type type, long batchSize)
      throws IllegalAccessException, InvocationTargetException, InstantiationException {
    TypeProtos.MajorType majorType = null;
    Class<? extends OrcColumnReader> readerClass = null;
    switch (type.getKind()) {
      case BOOLEAN:
        majorType =  Types.optional(TypeProtos.MinorType.BIT);
        readerClass = BooleanOrcColumnReader.class;
        break;
      case DOUBLE:
        majorType = Types.optional(TypeProtos.MinorType.FLOAT8);
        readerClass = DoubleOrcColumnReader.class;
        break;
      case FLOAT:
        majorType = Types.optional(TypeProtos.MinorType.FLOAT4);
        readerClass = FloatOrcColumnReader.class;
        break;
      case SHORT:
        majorType = Types.optional(TypeProtos.MinorType.SMALLINT);
        readerClass = ShortOrcColumnReader.class;
        break;
      case INT:
        majorType = Types.optional(TypeProtos.MinorType.INT);
        readerClass = IntOrcColumnReader.class;
        break;
      case LONG:
        majorType = Types.optional(TypeProtos.MinorType.BIGINT);
        readerClass = LongOrcColumnReader.class;
        break;
      case STRING:
      case VARCHAR:
        majorType = Types.optional(TypeProtos.MinorType.VARCHAR);
        readerClass = VarcharOrcColumnReader.class;
        break;
      case BINARY:
        majorType = Types.optional(TypeProtos.MinorType.VARBINARY);
        readerClass = VarBytesOrcColumnReader.class;
        break;
      case LIST:
      case MAP:
      case UNION:
      default:
        throw new IllegalArgumentException("Unsupported type " + type.getKind());
    }

    MaterializedField field = MaterializedField.create(
        new SchemaPath(fieldName, ExpressionPosition.UNKNOWN),
        majorType
    );

    ValueVector newVector = TypeHelper.getNewVector(field, allocator);
    VectorHolder holder = new VectorHolder((int) batchSize, newVector);
    return (OrcColumnReader) readerClass.getConstructors()[0].newInstance(holder);
  }

  private void populateStructColumns(List<OrcProto.Type> types,
                                     OrcProto.Type currentStructType) throws SchemaChangeException, IllegalAccessException, InstantiationException, InvocationTargetException {
    List<String> fieldNames = currentStructType.getFieldNamesList();
    for (int i = 0; i < fieldNames.size(); ++i) {
      int subType = currentStructType.getSubtypes(i);
      OrcProto.Type type = types.get(subType);
      if (type.hasKind() && type.getKind().equals(OrcProto.Type.Kind.STRUCT)) {
        populateStructColumns(types, type);
      } else {
        OrcColumnReader orcColumnReader = createOrcColumnReader(fieldNames.get(i), allocator, type, batchSize);
        holders.add(orcColumnReader);
      }
    }
  }

  @Override
  public int next() {
    try {
      VectorizedRowBatch previousBatch = batch;
      batch = orcReader.nextBatch(previousBatch);
      if (batch != null) {
        handleBatch(previousBatch, batch);
      }
      return batch == null ? 0 : (int) batch.count();
    } catch (IOException | SchemaChangeException e) {
      throw new DrillRuntimeException(e);
    }
  }

  private void handleBatch(VectorizedRowBatch previousBatch, VectorizedRowBatch batch) throws SchemaChangeException {
    populateMutator(previousBatch, batch);

    for(int colId : batch.projectedColumns) {
      OrcColumnReader reader = checkNotNull(holders.get(colId));
      reader.getHolder().reset();
      reader.parseNextBatch(batch.count(), batch.cols[colId]);
    }
  }

  private void populateMutator(VectorizedRowBatch previousBatch, VectorizedRowBatch batch) throws SchemaChangeException {
    Set<Integer> previousCols = Sets.newHashSet();

    if(previousBatch != null) {
      for(int colId : previousBatch.projectedColumns) {
        previousCols.add(colId);
      }
    }

    Set<Integer> newCols = Sets.newHashSet();

    for(int colId : batch.projectedColumns) {
      newCols.add(colId);
    }

    for(int newColId : Sets.difference(newCols, previousCols)) {
      mutator.addField(holders.get(newColId).getHolder().getValueVector());
    }

    for(int oldColId : Sets.difference(previousCols, newCols)) {
      mutator.removeField(holders.get(oldColId).getHolder().getValueVector().getField());
    }
  }

  @Override
  public void cleanup() {
    try {
      orcReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
