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

import io.netty.buffer.DrillBuf;

import java.lang.reflect.Field;
import java.sql.Timestamp;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarCharVector;

import com.google.common.base.Charsets;

public class Writers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Writers.class);

  public static class IntWriter extends AbstractWriter<IntVector> {

    public IntWriter(Field field) {
      super(field, Types.required(MinorType.INT));
      if (field.getType() != int.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      int i = field.getInt(pojo);
      vector.getMutator().setSafe(outboundIndex, i);
    }

  }

  public static class BitWriter extends AbstractWriter<BitVector>{

    public BitWriter(Field field) {
      super(field, Types.required(MinorType.BIT));
      if (field.getType() != boolean.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      boolean b = field.getBoolean(pojo);
      vector.getMutator().setSafe(outboundIndex, b ? 1 : 0);
    }

  }

  public static class LongWriter extends AbstractWriter<BigIntVector>{

    public LongWriter(Field field) {
      super(field, Types.required(MinorType.BIGINT));
      if (field.getType() != long.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      long l = field.getLong(pojo);
      vector.getMutator().setSafe(outboundIndex, l);
    }

  }

  public static class DoubleWriter extends AbstractWriter<Float8Vector>{

    public DoubleWriter(Field field) {
      super(field, Types.required(MinorType.FLOAT8));
      if (field.getType() != double.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      double d = field.getDouble(pojo);

      vector.getMutator().setSafe(outboundIndex, d);
    }

  }

  private abstract static class AbstractStringWriter extends AbstractWriter<NullableVarCharVector>{
    private DrillBuf data;
    private final NullableVarCharHolder h = new NullableVarCharHolder();

    public AbstractStringWriter(Field field, DrillBuf managedBuf) {
      super(field, Types.optional(MinorType.VARCHAR));
      this.data = managedBuf;
      ensureLength(100);
    }

    void ensureLength(int len) {
      data = data.reallocIfNeeded(len);
    }

    @Override
    public void cleanup() {
    }

    public void writeString(String s, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      if (s == null) {
        return;
      } else {
        h.isSet = 1;
        byte[] bytes = s.getBytes(Charsets.UTF_8);
        ensureLength(bytes.length);
        data.clear();
        data.writeBytes(bytes);
        h.buffer = data;
        h.start = 0;
        h.end = bytes.length;
        vector.getMutator().setSafe(outboundIndex, h);
      }
    }

  }

  public static class EnumWriter extends AbstractStringWriter{
    public EnumWriter(Field field, DrillBuf managedBuf) {
      super(field, managedBuf);
      if (!field.getType().isEnum()) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Enum<?> e= ((Enum<?>) field.get(pojo));
      if (e == null) {
        return;
      }
      writeString(e.name(), outboundIndex);
    }
  }

  public static class StringWriter extends AbstractStringWriter {
    public StringWriter(Field field, DrillBuf managedBuf) {
      super(field, managedBuf);
      if (field.getType() != String.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      String s = (String) field.get(pojo);
      writeString(s, outboundIndex);
    }
  }

  public static class NIntWriter extends AbstractWriter<NullableIntVector>{

    public NIntWriter(Field field) {
      super(field, Types.optional(MinorType.INT));
      if (field.getType() != Integer.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Integer i = (Integer) field.get(pojo);
      if (i != null) {
        vector.getMutator().setSafe(outboundIndex, i);
      }
    }

  }

  public static class NBigIntWriter extends AbstractWriter<NullableBigIntVector>{

    public NBigIntWriter(Field field) {
      super(field, Types.optional(MinorType.BIGINT));
      if (field.getType() != Long.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Long o = (Long) field.get(pojo);
      if (o != null) {
        vector.getMutator().setSafe(outboundIndex, o);
      }
    }

  }

  public static class NBooleanWriter extends AbstractWriter<NullableBitVector>{

    public NBooleanWriter(Field field) {
      super(field, Types.optional(MinorType.BIT));
      if (field.getType() != Boolean.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Boolean o = (Boolean) field.get(pojo);
      if (o != null) {
        vector.getMutator().setSafe(outboundIndex, o ? 1 : 0);
      }
    }

  }
  public static class NDoubleWriter extends AbstractWriter<NullableFloat8Vector>{

    public NDoubleWriter(Field field) {
      super(field, Types.optional(MinorType.FLOAT8));
      if (field.getType() != Double.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Double o = (Double) field.get(pojo);
      if (o != null) {
        vector.getMutator().setSafe(outboundIndex, o);
      }
    }

  }

  public static class NTimeStampWriter extends AbstractWriter<NullableTimeStampVector>{

    public NTimeStampWriter(Field field) {
      super(field, Types.optional(MinorType.TIMESTAMP));
      if (field.getType() != Timestamp.class) {
        throw new IllegalStateException();
      }
    }

    @Override
    public void writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException {
      Timestamp o = (Timestamp) field.get(pojo);
      if (o != null) {
        vector.getMutator().setSafe(outboundIndex, o.getTime());
      }
    }
  }
}
