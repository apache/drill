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
package org.apache.drill.exec.vector.complex.impl;

import java.util.Iterator;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.vector.complex.RepeatedStructVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.StructWriter;

public class SingleLikeRepeatedStructReaderImpl extends AbstractFieldReader{

  private RepeatedStructReaderImpl delegate;

  public SingleLikeRepeatedStructReaderImpl(RepeatedStructVector vector, FieldReader delegate) {
    this.delegate = (RepeatedStructReaderImpl) delegate;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("You can't call size on a single struct reader.");
  }

  @Override
  public boolean next() {
    throw new UnsupportedOperationException("You can't call next on a single struct reader.");
  }

  @Override
  public MajorType getType() {
    return Types.required(MinorType.STRUCT);
  }


  @Override
  public void copyAsValue(StructWriter writer) {
    delegate.copyAsValueSingle(writer);
  }

  public void copyAsValueSingle(StructWriter writer){
    delegate.copyAsValueSingle(writer);
  }

  @Override
  public FieldReader reader(String name) {
    return delegate.reader(name);
  }

  @Override
  public void setPosition(int index) {
    delegate.setPosition(index);
  }

  @Override
  public Object readObject() {
    return delegate.readObject();
  }

  @Override
  public Iterator<String> iterator() {
    return delegate.iterator();
  }

  @Override
  public boolean isSet() {
    return ! delegate.isNull();
  }

}
