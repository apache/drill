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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.RepeatedListHolder;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;

public class RepeatedListReaderImpl extends AbstractFieldReader{
  private static final int NO_VALUES = Integer.MAX_VALUE - 1;
  private static final MajorType TYPE = Types.repeated(MinorType.LIST);
  private final String name;
  private final RepeatedListVector container;
  private FieldReader reader;

  public RepeatedListReaderImpl(String name, RepeatedListVector container) {
    super();
    this.name = name;
    this.container = container;
  }

  @Override
  public MajorType getType() {
    return TYPE;
  }

  @Override
  public void copyAsValue(ListWriter writer) {
    if (currentOffset == NO_VALUES) {
      return;
    }
    RepeatedListWriter impl = (RepeatedListWriter) writer;
    impl.inform(impl.container.copyFromSafe(idx(), impl.idx(), container));
  }

  @Override
  public void copyAsField(String name, MapWriter writer) {
    if (currentOffset == NO_VALUES) {
      return;
    }
    RepeatedListWriter impl = (RepeatedListWriter) writer.list(name);
    impl.inform(impl.container.copyFromSafe(idx(), impl.idx(), container));
  }

  private int currentOffset;
  private int maxOffset;

  @Override
  public int size() {
    return maxOffset - currentOffset;
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    RepeatedListHolder h = new RepeatedListHolder();
    container.getAccessor().get(index, h);
    if (h.start == h.end) {
      currentOffset = NO_VALUES;
    } else {
      currentOffset = h.start-1;
      maxOffset = h.end;
      if(reader != null) {
        reader.setPosition(currentOffset);
      }
    }
  }

  @Override
  public boolean next() {
    if (currentOffset +1 < maxOffset) {
      currentOffset++;
      if (reader != null) {
        reader.setPosition(currentOffset);
      }
      return true;
    } else {
      currentOffset = NO_VALUES;
      return false;
    }
  }

  @Override
  public Object readObject() {
    return container.getAccessor().getObject(idx());
  }

  @Override
  public FieldReader reader() {
    if (reader == null) {
      reader = container.get(name, ValueVector.class).getAccessor().getReader();
      if (currentOffset == NO_VALUES) {
        reader = NullReader.INSTANCE;
      } else {
        reader.setPosition(currentOffset);
      }
    }
    return reader;
  }

  public boolean isSet() {
    return true;
  }

}
