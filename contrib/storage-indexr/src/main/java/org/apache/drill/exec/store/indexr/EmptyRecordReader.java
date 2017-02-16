/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.ValueVector;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SQLType;
import io.indexr.segment.SegmentSchema;

public class EmptyRecordReader extends AbstractRecordReader {
  private SegmentSchema schema;

  public EmptyRecordReader(SegmentSchema schema) {
    this.schema = schema;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    for (ColumnSchema cs : schema.columns) {
      addFeild(cs.getSqlType(), cs.name, output);
    }
  }

  @SuppressWarnings("unchecked")
  private void addFeild(SQLType dataType, String name, OutputMutator output) {
    TypeProtos.MinorType minorType = DrillIndexRTable.parseMinorType(dataType);
    TypeProtos.MajorType majorType = Types.required(minorType);
    MaterializedField field = MaterializedField.create(name, majorType);
    final Class<? extends ValueVector> clazz = (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(minorType, majorType.getMode());
    try {
      output.addField(field, clazz).allocateNew();
    } catch (SchemaChangeException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int next() {
    return 0;
  }

  @Override
  public void close() throws Exception {
  }
}