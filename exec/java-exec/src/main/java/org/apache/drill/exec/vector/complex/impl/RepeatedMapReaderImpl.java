

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


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.common.base.Charsets;
import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.types.TypeProtos.*;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.vector.complex.*;
import org.apache.drill.exec.vector.complex.reader.*;
import org.apache.drill.exec.vector.complex.writer.*;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;

import com.sun.codemodel.JType;
import com.sun.codemodel.JCodeModel;

import java.util.Arrays;
import java.util.Random;
import java.util.List;
import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.apache.hadoop.io.Text;













import java.util.Map;
import java.util.Map.Entry;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.complex.MapVector;

import com.google.common.collect.Maps;

@SuppressWarnings("unused")
public class RepeatedMapReaderImpl extends AbstractFieldReader{
  private static final int NO_VALUES = Integer.MAX_VALUE - 1;

  private final RepeatedMapVector vector;
  private final Map<String, FieldReader> fields = Maps.newHashMap();

  public RepeatedMapReaderImpl(RepeatedMapVector vector) {
    this.vector = vector;
  }

  private void setChildrenPosition(int index){
    for(FieldReader r : fields.values()){
      r.setPosition(index);
    }
  }

  public FieldReader reader(String name){
    FieldReader reader = fields.get(name);
    if(reader == null){
      ValueVector child = vector.get(name, ValueVector.class);
      if(child == null){
        reader = NullReader.INSTANCE;
      }else{
        reader = child.getAccessor().getReader();
      }
      fields.put(name, reader);
      reader.setPosition(currentOffset);
    }
    return reader;
  }

  public FieldReader reader() {
    if (currentOffset == NO_VALUES) 
      return NullReader.INSTANCE;
    
    setChildrenPosition(currentOffset);
    return new SingleLikeRepeatedMapReaderImpl(vector, this);
  }

  private int currentOffset;
  private int maxOffset;

  public int size(){
    return maxOffset - currentOffset;
  }

  public void setPosition(int index){
    super.setPosition(index);
    RepeatedMapHolder h = new RepeatedMapHolder();
    vector.getAccessor().get(index, h);
    if(h.start == h.end){
      currentOffset = NO_VALUES;
    }else{
      currentOffset = h.start-1;
      maxOffset = h.end;
      setChildrenPosition(currentOffset);
    }
  }

  public void setSinglePosition(int index, int childIndex){
    super.setPosition(index);
    RepeatedMapHolder h = new RepeatedMapHolder();
    vector.getAccessor().get(index, h);
    if(h.start == h.end){
      currentOffset = NO_VALUES;
    }else{
      int singleOffset = h.start + childIndex;
      assert singleOffset < h.end;
      currentOffset = singleOffset;
      maxOffset = singleOffset + 1;
      setChildrenPosition(singleOffset);
    }
  }

  public boolean next(){
    if(currentOffset +1 < maxOffset){
      setChildrenPosition(++currentOffset);
      return true;
    }else{
      currentOffset = NO_VALUES;
      return false;
    }
  }

  public boolean isNull() {
    return currentOffset == NO_VALUES;
  }

  @Override
  public Object readObject() {
    return vector.getAccessor().getObject(idx());
  }

  public MajorType getType(){
    return vector.getField().getType();
  }

  public java.util.Iterator<String> iterator(){
    return vector.fieldNameIterator();
  }

  @Override
  public boolean isSet() {
    return true;
  }

  public void copyAsValue(MapWriter writer){
    if(currentOffset == NO_VALUES) return;
    RepeatedMapWriter impl = (RepeatedMapWriter) writer;
    impl.inform(impl.container.copyFromSafe(idx(), impl.idx(), vector));
  }

  public void copyAsValueSingle(MapWriter writer){
    if(currentOffset == NO_VALUES) return;
    SingleMapWriter impl = (SingleMapWriter) writer;
    impl.inform(impl.container.copyFromSafe(currentOffset, impl.idx(), vector));
  }

  public void copyAsField(String name, MapWriter writer){
    if(currentOffset == NO_VALUES) return;
    RepeatedMapWriter impl = (RepeatedMapWriter) writer.map(name);
    impl.inform(impl.container.copyFromSafe(idx(), impl.idx(), vector));
  }


}


