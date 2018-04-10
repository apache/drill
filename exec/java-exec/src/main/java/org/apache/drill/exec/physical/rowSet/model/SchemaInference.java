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
package org.apache.drill.exec.physical.rowSet.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;

/**
 * Produce a metadata schema from a vector container. Used when given a
 * record batch without metadata.
 */

public class SchemaInference {

  public TupleMetadata infer(VectorContainer container) {
    List<ColumnMetadata> columns = new ArrayList<>();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      MaterializedField field = container.getValueVector(i).getField();
      columns.add(inferVector(field));
    }
    return MetadataUtils.fromColumns(columns);
  }

  private ColumnMetadata inferVector(MaterializedField field) {
    if (field.getType().getMinorType() == MinorType.MAP) {
      return MetadataUtils.newMap(field, inferMapSchema(field));
    } else {
      return MetadataUtils.fromField(field);
    }
  }

  private TupleSchema inferMapSchema(MaterializedField field) {
    List<ColumnMetadata> columns = new ArrayList<>();
    for (MaterializedField child : field.getChildren()) {
      columns.add(inferVector(child));
    }
    return MetadataUtils.fromColumns(columns);
  }
}
