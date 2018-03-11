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
package org.apache.drill.test.rowSet.schema;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.metadata.AbstractColumnMetadata;
import org.apache.drill.exec.record.metadata.VariantColumnMetadata;
import org.apache.drill.exec.record.metadata.VariantSchema;

/**
 * Builds unions or (non-repeated) lists (which implicitly contain
 * unions.)
 */

public class UnionBuilder implements SchemaContainer {
  private final SchemaContainer parent;
  private final String name;
  private final MinorType type;
  private final VariantSchema union;

  public UnionBuilder(SchemaContainer parent, String name,
      MinorType type, DataMode mode) {
    this.parent = parent;
    this.name = name;
    this.type = type;
    union = new VariantSchema();
  }

  private void checkType(MinorType type) {
    if (union.hasType(type)) {
      throw new IllegalArgumentException("Duplicate type: " + type);
    }
  }

  @Override
  public void addColumn(AbstractColumnMetadata column) {
    assert column.name().equals(Types.typeKey(column.type()));
    union.addType(column);
  }

  public UnionBuilder addType(MinorType type) {
    checkType(type);
    union.addType(type);
    return this;
  }

  public MapBuilder addMap() {
    checkType(MinorType.MAP);
    return new MapBuilder(this, Types.typeKey(MinorType.MAP), DataMode.OPTIONAL);
  }

  public UnionBuilder addList() {
    checkType(MinorType.LIST);
    return new UnionBuilder(this, Types.typeKey(MinorType.LIST),
        MinorType.LIST, DataMode.OPTIONAL);
  }

  public RepeatedListBuilder addRepeatedList() {
    checkType(MinorType.LIST);
    return new RepeatedListBuilder(this, Types.typeKey(MinorType.LIST));
  }

  private VariantColumnMetadata buildCol() {
    return new VariantColumnMetadata(name, type, union);
  }

  public SchemaBuilder resumeSchema() {
    parent.addColumn(buildCol());
    return (SchemaBuilder) parent;
  }

  public UnionBuilder buildNested() {
    parent.addColumn(buildCol());
    return (UnionBuilder) parent;
  }

  public MapBuilder resumeMap() {
    parent.addColumn(buildCol());
    return (MapBuilder) parent;
  }

  public UnionBuilder resumeUnion() {
    parent.addColumn(buildCol());
    return (UnionBuilder) parent;
  }
}