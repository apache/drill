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
package org.apache.drill.exec.physical.impl.scan.project;

import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple.ResolvedStruct;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple.ResolvedMapArray;
import org.apache.drill.exec.physical.impl.scan.project.ResolvedTuple.ResolvedSingleMap;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Represents a column which is implicitly a struct (because it has children
 * in the project list), but which does not match any column in the table.
 * This kind of column gives rise to a struct of null columns in the output.
 */

public class ResolvedStructColumn extends ResolvedColumn {

  private final MaterializedField schema;
  private final ResolvedTuple parent;
  private final ResolvedStruct members;

  public ResolvedStructColumn(ResolvedTuple parent, String name) {
    super(parent, -1);
    schema = MaterializedField.create(name,
        Types.required(MinorType.STRUCT));
    this.parent = parent;
    members = new ResolvedSingleMap(this);
    parent.addChild(members);
  }

  public ResolvedStructColumn(ResolvedTuple parent,
                              MaterializedField schema, int sourceIndex) {
    super(parent, sourceIndex);
    this.schema = schema;
    this.parent = parent;

    // This column corresponds to an input struct.
    // We may have to create a matching new output
    // struct. Determine whether it is a single struct or
    // an array.

    assert schema.getType().getMinorType() == MinorType.STRUCT;
    if (schema.getType().getMode() == DataMode.REPEATED) {
      members = new ResolvedMapArray(this);
    } else {
      members = new ResolvedSingleMap(this);
    }
    parent.addChild(members);
  }

  public ResolvedTuple parent() { return parent; }

  @Override
  public String name() { return schema.getName(); }

  public ResolvedTuple members() { return members; }

  @Override
  public void project(ResolvedTuple dest) {
    dest.addVector(members.buildMap());
  }

  @Override
  public MaterializedField schema() { return schema; }
}
