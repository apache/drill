/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.record;

import java.util.Iterator;
import java.util.List;


public class BatchSchema implements Iterable<MaterializedField> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchSchema.class);
  final SelectionVectorMode selectionVector;
  ;
  private final List<MaterializedField> fields;

  BatchSchema(SelectionVectorMode selectionVector, List<MaterializedField> fields) {
    this.fields = fields;
    this.selectionVector = selectionVector;
  }

  public static SchemaBuilder newBuilder() {
    return new SchemaBuilder();
  }

  @Override
  public Iterator<MaterializedField> iterator() {
    return fields.iterator();
  }

  public SelectionVectorMode getSelectionVector() {
    return selectionVector;
  }

  @Override
  public String toString() {
    return "BatchSchema [fields=" + fields + ", selectionVector=" + selectionVector + "]";
  }

  public static enum SelectionVectorMode {
    NONE(-1, false), TWO_BYTE(2, true), FOUR_BYTE(4, true);

    public boolean hasSelectionVector;
    public final int size;
    SelectionVectorMode(int size, boolean hasSelectionVector) {
      this.size = size;
    }
  }


}
