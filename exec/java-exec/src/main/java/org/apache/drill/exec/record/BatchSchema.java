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
package org.apache.drill.exec.record;


import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.drill.common.types.TypeProtos.MajorType;


public class BatchSchema implements Iterable<MaterializedField> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchSchema.class);
  final SelectionVectorMode selectionVectorMode;
  private final List<MaterializedField> fields;

  BatchSchema(SelectionVectorMode selectionVector, List<MaterializedField> fields) {
    this.fields = fields;
    this.selectionVectorMode = selectionVector;
  }

  public static SchemaBuilder newBuilder() {
    return new SchemaBuilder();
  }

  public int getFieldCount() {
    return fields.size();
  }

  public MaterializedField getColumn(int index) {
    if (index < 0 || index >= fields.size()) {
      return null;
    }
    return fields.get(index);
  }

  @Override
  public Iterator<MaterializedField> iterator() {
    return fields.iterator();
  }

  public SelectionVectorMode getSelectionVectorMode() {
    return selectionVectorMode;
  }

  @Override
  public BatchSchema clone() {
    List<MaterializedField> newFields = Lists.newArrayList();
    newFields.addAll(fields);
    return new BatchSchema(selectionVectorMode, newFields);
  }

  @Override
  public String toString() {
    return "BatchSchema [fields=" + fields + ", selectionVector=" + selectionVectorMode + "]";
  }

  public static enum SelectionVectorMode {
    NONE(-1, false), TWO_BYTE(2, true), FOUR_BYTE(4, true);

    public boolean hasSelectionVector;
    public final int size;
    SelectionVectorMode(int size, boolean hasSelectionVector) {
      this.size = size;
    }

    public static SelectionVectorMode[] DEFAULT = {NONE};
    public static SelectionVectorMode[] NONE_AND_TWO = {NONE, TWO_BYTE};
    public static SelectionVectorMode[] NONE_AND_FOUR = {NONE, FOUR_BYTE};
    public static SelectionVectorMode[] ALL = {NONE, TWO_BYTE, FOUR_BYTE};
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fields == null) ? 0 : fields.hashCode());
    result = prime * result + ((selectionVectorMode == null) ? 0 : selectionVectorMode.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BatchSchema other = (BatchSchema) obj;
    if (fields == null) {
      if (other.fields != null) {
        return false;
      }
    } else if (!fields.equals(other.fields)) {
      return false;
    }
    for (int i = 0; i < fields.size(); i++) {
      MajorType t1 = fields.get(i).getType();
      MajorType t2 = other.fields.get(i).getType();
      if (t1 == null) {
        if (t2 != null) {
          return false;
        }
      } else {
        if (!majorTypeEqual(t1, t2)) {
          return false;
        }
      }
    }
    if (selectionVectorMode != other.selectionVectorMode) {
      return false;
    }
    return true;
  }

  /**
   * We treat fields with same set of Subtypes as equal, even if they are in a different order
   * @param t1
   * @param t2
   * @return
   */
  private boolean majorTypeEqual(MajorType t1, MajorType t2) {
    if (t1.equals(t2)) {
      return true;
    }
    if (!t1.getMinorType().equals(t2.getMinorType())) {
      return false;
    }
    if (!t1.getMode().equals(t2.getMode())) {
      return false;
    }
    if (!Sets.newHashSet(t1.getSubTypeList()).equals(Sets.newHashSet(t2.getSubTypeList()))) {
      return false;
    }
    return true;
  }

}
