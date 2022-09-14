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
package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BigIntVector;

public class InsertWriterRecordBatch extends WriterRecordBatch {

  public InsertWriterRecordBatch(Writer writer, RecordBatch incoming,
    FragmentContext context, RecordWriter recordWriter) throws OutOfMemoryException {
    super(writer, incoming, context, recordWriter);
  }

  @Override
  protected void addOutputContainerData() {
    BigIntVector rowcountVector = (BigIntVector) container.getValueAccessorById(BigIntVector.class,
        container.getValueVectorId(SchemaPath.getSimplePath("ROWCOUNT")).getFieldIds())
      .getValueVector();
    AllocationHelper.allocate(rowcountVector, 1, 8);
    rowcountVector.getMutator().setSafe(0, counter);
    rowcountVector.getMutator().setValueCount(1);

    container.setRecordCount(1);
  }

  protected void addOutputSchema() {
    // Create vector for ROWCOUNT - number of records written.
    final MaterializedField rowcountField =
      MaterializedField.create("ROWCOUNT",
        Types.required(TypeProtos.MinorType.BIGINT));

    container.addOrGet(rowcountField);
  }
}
