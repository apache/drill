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
package org.apache.drill.exec.vector.complex.impl;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.holders.UnionHolder;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

public class UnionListReader extends AbstractFieldReader {

  private ListVector vector;
  private UnionVector data;
  private UInt4Vector offsets;
  private UnionReader reader;

  public UnionListReader(ListVector vector) {
    this.vector = vector;
    this.data = vector.getDataVector();
    this.offsets = vector.getOffsetVector();
    this.reader = (UnionReader) data.getReader();
  }

  @Override
  public boolean isSet() {
    return true;
  }

  @Override
  public MajorType getType() {
    return reader.getType();
  }

  private int currentOffset;
  private int maxOffset;

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    currentOffset = offsets.getAccessor().get(index) - 1;
    maxOffset = offsets.getAccessor().get(index + 1);
  }

  @Override
  public FieldReader reader() {
    return reader;
  }

  @Override
  public Object readObject() {
    return vector.getAccessor().getObject(idx());
  }

  @Override
  public void read(int index, UnionHolder holder) {
    setPosition(idx());
    for (int i = -1; i < index; i++) {
      next();
    }
    holder.reader = reader;
    holder.isSet = reader.isSet() ? 1 : 0;
  }

  @Override
  public boolean next() {
    if (currentOffset + 1 < maxOffset) {
      reader.setPosition(++currentOffset);
      return true;
    } else {
      return false;
    }
  }
}
