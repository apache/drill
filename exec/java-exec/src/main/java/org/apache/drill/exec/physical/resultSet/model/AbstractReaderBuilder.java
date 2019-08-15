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
package org.apache.drill.exec.physical.resultSet.model;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.ArrayReaderImpl;
import org.apache.drill.exec.vector.accessor.reader.BaseScalarReader;
import org.apache.drill.exec.vector.accessor.reader.ColumnReaderFactory;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;

public abstract class AbstractReaderBuilder {

  protected AbstractObjectReader buildScalarReader(VectorAccessor va, ColumnMetadata schema) {
    BaseScalarReader scalarReader = ColumnReaderFactory.buildColumnReader(va);
    DataMode mode = va.type().getMode();
    switch (mode) {
    case OPTIONAL:
      return BaseScalarReader.buildOptional(schema, va, scalarReader);
    case REQUIRED:
      return BaseScalarReader.buildRequired(schema, va, scalarReader);
    case REPEATED:
      return ArrayReaderImpl.buildScalar(schema, va, scalarReader);
    default:
      throw new UnsupportedOperationException(mode.toString());
    }
  }

}
