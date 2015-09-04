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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Collection;
import java.util.List;

public class DrillParquetRecordMaterializer extends RecordMaterializer<Void> {

  public DrillParquetGroupConverter root;
  private ComplexWriter complexWriter;

  public DrillParquetRecordMaterializer(OutputMutator mutator, ComplexWriter complexWriter, MessageType schema,
                                        Collection<SchemaPath> columns, OptionManager options) {
    this.complexWriter = complexWriter;
    root = new DrillParquetGroupConverter(mutator, complexWriter.rootAsMap(), schema, columns, options);
  }

  public void setPosition(int position) {
    complexWriter.setPosition(position);
  }

  @Override
  public Void getCurrentRecord() {
    return null;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
