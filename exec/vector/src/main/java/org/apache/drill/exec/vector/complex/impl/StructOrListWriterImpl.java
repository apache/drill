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

import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.StructOrListWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.BitWriter;
import org.apache.drill.exec.vector.complex.writer.DateWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal18Writer;
import org.apache.drill.exec.vector.complex.writer.Decimal28DenseWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal28SparseWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal38DenseWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal38SparseWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal9Writer;
import org.apache.drill.exec.vector.complex.writer.VarDecimalWriter;
import org.apache.drill.exec.vector.complex.writer.Float4Writer;
import org.apache.drill.exec.vector.complex.writer.Float8Writer;
import org.apache.drill.exec.vector.complex.writer.IntWriter;
import org.apache.drill.exec.vector.complex.writer.IntervalDayWriter;
import org.apache.drill.exec.vector.complex.writer.IntervalWriter;
import org.apache.drill.exec.vector.complex.writer.IntervalYearWriter;
import org.apache.drill.exec.vector.complex.writer.SmallIntWriter;
import org.apache.drill.exec.vector.complex.writer.TimeStampWriter;
import org.apache.drill.exec.vector.complex.writer.TimeWriter;
import org.apache.drill.exec.vector.complex.writer.TinyIntWriter;
import org.apache.drill.exec.vector.complex.writer.UInt1Writer;
import org.apache.drill.exec.vector.complex.writer.UInt2Writer;
import org.apache.drill.exec.vector.complex.writer.UInt4Writer;
import org.apache.drill.exec.vector.complex.writer.UInt8Writer;
import org.apache.drill.exec.vector.complex.writer.Var16CharWriter;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;

public class StructOrListWriterImpl implements StructOrListWriter {

  public final BaseWriter.StructWriter struct;
  public final BaseWriter.ListWriter list;

  public StructOrListWriterImpl(final BaseWriter.StructWriter writer) {
    this.struct = writer;
    this.list = null;
  }

  public StructOrListWriterImpl(final BaseWriter.ListWriter writer) {
    this.struct = null;
    this.list = writer;
  }

  @Override
  public void start() {
    if (struct != null) {
      struct.start();
    } else {
      list.startList();
    }
  }

  @Override
  public void end() {
    if (struct != null) {
      struct.end();
    } else {
      list.endList();
    }
  }

  @Override
  public StructOrListWriter struct(final String name) {
    assert struct != null;
    return new StructOrListWriterImpl(struct.struct(name));
  }

  @Override
  public StructOrListWriter listoftstruct(final String name) {
    assert list != null;
    return new StructOrListWriterImpl(list.struct());
  }

  @Override
  public StructOrListWriter list(final String name) {
    assert struct != null;
    return new StructOrListWriterImpl(struct.list(name));
  }

  @Override
  public boolean isStructWriter() {
    return struct != null;
  }

  @Override
  public boolean isListWriter() {
    return list != null;
  }

  @Override
  public VarCharWriter varChar(final String name) {
    return (struct != null) ? struct.varChar(name) : list.varChar();
  }

  @Override
  public IntWriter integer(final String name) {
    return (struct != null) ? struct.integer(name) : list.integer();
  }

  @Override
  public BigIntWriter bigInt(final String name) {
    return (struct != null) ? struct.bigInt(name) : list.bigInt();
  }

  @Override
  public Float4Writer float4(final String name) {
    return (struct != null) ? struct.float4(name) : list.float4();
  }

  @Override
  public Float8Writer float8(final String name) {
    return (struct != null) ? struct.float8(name) : list.float8();
  }

  @Override
  public BitWriter bit(final String name) {
    return (struct != null) ? struct.bit(name) : list.bit();
  }

  /**
   * {@inheritDoc}
   */
  @Deprecated
  @Override
  public VarBinaryWriter binary(final String name) {
    return (struct != null) ? struct.varBinary(name) : list.varBinary();
  }

  @Override
  public TinyIntWriter tinyInt(String name) {
    return (struct != null) ? struct.tinyInt(name) : list.tinyInt();
  }

  @Override
  public SmallIntWriter smallInt(String name) {
    return (struct != null) ? struct.smallInt(name) : list.smallInt();
  }

  @Override
  public DateWriter date(String name) {
    return (struct != null) ? struct.date(name) : list.date();
  }

  @Override
  public TimeWriter time(String name) {
    return (struct != null) ? struct.time(name) : list.time();
  }

  @Override
  public TimeStampWriter timeStamp(String name) {
    return (struct != null) ? struct.timeStamp(name) : list.timeStamp();
  }

  @Override
  public VarBinaryWriter varBinary(String name) {
    return (struct != null) ? struct.varBinary(name) : list.varBinary();
  }

  @Override
  public Var16CharWriter var16Char(String name) {
    return (struct != null) ? struct.var16Char(name) : list.var16Char();
  }

  @Override
  public UInt1Writer uInt1(String name) {
    return (struct != null) ? struct.uInt1(name) : list.uInt1();
  }

  @Override
  public UInt2Writer uInt2(String name) {
    return (struct != null) ? struct.uInt2(name) : list.uInt2();
  }

  @Override
  public UInt4Writer uInt4(String name) {
    return (struct != null) ? struct.uInt4(name) : list.uInt4();
  }

  @Override
  public UInt8Writer uInt8(String name) {
    return (struct != null) ? struct.uInt8(name) : list.uInt8();
  }

  @Override
  public IntervalYearWriter intervalYear(String name) {
    return (struct != null) ? struct.intervalYear(name) : list.intervalYear();
  }

  @Override
  public IntervalDayWriter intervalDay(String name) {
    return (struct != null) ? struct.intervalDay(name) : list.intervalDay();
  }

  @Override
  public IntervalWriter interval(String name) {
    return (struct != null) ? struct.interval(name) : list.interval();
  }

  @Override
  public Decimal9Writer decimal9(String name) {
    return (struct != null) ? struct.decimal9(name) : list.decimal9();
  }

  @Override
  public Decimal18Writer decimal18(String name) {
    return (struct != null) ? struct.decimal18(name) : list.decimal18();
  }

  @Override
  public Decimal28DenseWriter decimal28Dense(String name) {
    return (struct != null) ? struct.decimal28Dense(name) : list.decimal28Dense();
  }

  @Override
  public Decimal38DenseWriter decimal38Dense(String name) {
    return (struct != null) ? struct.decimal38Dense(name) : list.decimal38Dense();
  }

  @Override
  public VarDecimalWriter varDecimal(String name) {
    return (struct != null) ? struct.varDecimal(name) : list.varDecimal();
  }

  @Override
  public VarDecimalWriter varDecimal(String name, int scale, int precision) {
    return (struct != null) ? struct.varDecimal(name, scale, precision) : list.varDecimal(scale, precision);
  }

  @Override
  public Decimal38SparseWriter decimal38Sparse(String name) {
    return (struct != null) ? struct.decimal38Sparse(name) : list.decimal38Sparse();
  }

  @Override
  public Decimal28SparseWriter decimal28Sparse(String name) {
    return (struct != null) ? struct.decimal28Sparse(name) : list.decimal28Sparse();
  }

}
