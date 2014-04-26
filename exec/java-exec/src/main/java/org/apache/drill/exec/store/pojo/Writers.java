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
package org.apache.drill.exec.store.pojo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.lang.reflect.Field;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;

import com.google.common.base.Charsets;

public class Writers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Writers.class);

  public static class IntWriter extends AbstractWriter<IntVector>{

    public IntWriter(Field field) {
      super(field, Types.required(MinorType.INT));
      if(field.getType() != int.class) throw new IllegalStateException();
    }

    @Override
    public boolean writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      int i = field.getInt(pojo);
      return vector.getMutator().setSafe(outboundIndex, i);
    }

  }

  public static class BitWriter extends AbstractWriter<BitVector>{

    public BitWriter(Field field) {
      super(field, Types.required(MinorType.BIT));
      if(field.getType() != boolean.class) throw new IllegalStateException();
    }

    @Override
    public boolean writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      boolean b = field.getBoolean(pojo);
      return vector.getMutator().setSafe(outboundIndex, b ? 1 : 0);
    }

  }

  public static class LongWriter extends AbstractWriter<BigIntVector>{

    public LongWriter(Field field) {
      super(field, Types.required(MinorType.BIGINT));
      if(field.getType() != long.class) throw new IllegalStateException();
    }

    @Override
    public boolean writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      long l = field.getLong(pojo);

      return vector.getMutator().setSafe(outboundIndex, l);
    }

  }

  public static class StringWriter extends AbstractWriter<NullableVarCharVector>{

    private ByteBuf data;
    private final NullableVarCharHolder h = new NullableVarCharHolder();

    public StringWriter(Field field) {
      super(field, Types.optional(MinorType.VARCHAR));
      if(field.getType() != String.class) throw new IllegalStateException();
      ensureLength(100);
    }

    private void ensureLength(int len){
      if(data == null || data.capacity() < len){
        if(data != null) data.release();
        data = UnpooledByteBufAllocator.DEFAULT.buffer(len);
      }
    }

    public void cleanup(){
      data.release();
    }

    @Override
    public boolean writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      String s = (String) field.get(pojo);
      if(s == null){
        h.isSet = 0;
      }else{
        h.isSet = 1;
        byte[] bytes = s.getBytes(Charsets.UTF_8);
        ensureLength(bytes.length);
        data.clear();
        data.writeBytes(bytes);
        h.buffer = data;
        h.start = 0;
        h.end = bytes.length;

      }

      return vector.getMutator().setSafe(outboundIndex, h);
    }

  }
}
