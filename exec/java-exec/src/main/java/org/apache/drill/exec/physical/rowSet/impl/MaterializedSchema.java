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
package org.apache.drill.exec.physical.rowSet.impl;

import java.util.Iterator;

import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Similar to {@link BatchSchema}, except that
 * <ul>
 * <li>Provides fast lookup by name.</li>
 * <li>Does not carry a selection vector mode.</li>
 * </ul>
 * This form is useful when performing semantic analysis.
 */

public class MaterializedSchema implements Iterable<MaterializedField> {
  public final TupleNameSpace<MaterializedField> nameSpace = new TupleNameSpace<>();

  public MaterializedSchema() { }

  public MaterializedSchema(BatchSchema schema) {
    for (MaterializedField field : schema) {
      add(field);
    }
  }

  public MaterializedSchema(MaterializedSchema schema) {
    for (MaterializedField field : schema) {
      add(field);
    }
  }

  public void add(MaterializedField field) {
    nameSpace.add(field.getName(), field);
  }

  public MaterializedField column(String name) {
    return nameSpace.get(name);
  }

  public int index(String name) {
    return nameSpace.indexOf(name);
  }

  public MaterializedField column(int index) {
    return nameSpace.get(index);
  }

  public int size() { return nameSpace.count(); }

  public boolean isEmpty() { return nameSpace.count( ) == 0; }

  @Override
  public Iterator<MaterializedField> iterator() {
    return nameSpace.iterator();
  }

  public BatchSchema asBatchSchema() {
    return new BatchSchema(SelectionVectorMode.NONE, nameSpace.entries());
  }

  public boolean isEquivalent(MaterializedSchema other) {
    if (nameSpace.count() != other.nameSpace.count()) {
      return false;
    }
    for (int i = 0; i < nameSpace.count(); i++) {
      if (! nameSpace.get(i).isEquivalent(other.nameSpace.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return nameSpace.toString();
  }

  public MaterializedSchema merge(MaterializedSchema other) {
    MaterializedSchema merged = new MaterializedSchema(this);
    for (MaterializedField field : other) {
      merged.add(field);
    }
    return merged;
  }
}
