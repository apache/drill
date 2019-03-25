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
package org.apache.drill.exec.record.metadata;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.MaterializedField;

public class DictColumnMetadata extends AbstractMapColumnMetadata {

  /**
   * Build a new dict column from the field provided
   *
   * @param schema materialized field description of the map
   */
  public DictColumnMetadata(MaterializedField schema) {
    this(schema, null);
  }

  /**
   * Build a dict column metadata by cloning the type information (but not
   * the children) of the materialized field provided.
   *
   * @param schema the schema to use
   * @param mapSchema parent schema
   */
  DictColumnMetadata(MaterializedField schema, TupleSchema mapSchema) {
    super(schema, mapSchema);
  }

  public DictColumnMetadata(DictColumnMetadata from) {
    super(from);
  }

  public DictColumnMetadata(String name, TypeProtos.DataMode mode, TupleSchema mapSchema) {
    super(name, TypeProtos.MinorType.DICT, mode, mapSchema);
  }

  @Override
  public ColumnMetadata copy() {
    return new DictColumnMetadata(this);
  }

  @Override
  public ColumnMetadata cloneEmpty() {
    return new DictColumnMetadata(name, mode, new TupleSchema());
  }

  @Override
  public boolean isDict() {
    return true;
  }

  @Override
  protected String getStringType() {
    return "MAP";
  }
}
