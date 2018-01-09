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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapOrListWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.BitWriter;
import org.apache.drill.exec.vector.complex.writer.DateWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal18Writer;
import org.apache.drill.exec.vector.complex.writer.Decimal28DenseWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal28SparseWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal38DenseWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal38SparseWriter;
import org.apache.drill.exec.vector.complex.writer.Decimal9Writer;
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

public class MapOrListWriterImpl implements MapOrListWriter {

  public final BaseWriter.MapWriter map;
  public final BaseWriter.ListWriter list;

  public MapOrListWriterImpl(final BaseWriter.MapWriter writer) {
    this.map = writer;
    this.list = null;
  }

  public MapOrListWriterImpl(final BaseWriter.ListWriter writer) {
    this.map = null;
    this.list = writer;
  }

  public void start() {
    if (map != null) {
      map.start();
    } else {
      list.startList();
    }
  }

  public void end() {
    if (map != null) {
      map.end();
    } else {
      list.endList();
    }
  }

  public MapOrListWriter map(final String name) {
    assert map != null;
    return new MapOrListWriterImpl(map.map(name));
  }

  public MapOrListWriter listoftmap(final String name) {
    assert list != null;
    return new MapOrListWriterImpl(list.map());
  }

  public MapOrListWriter list(final String name) {
    assert map != null;
    return new MapOrListWriterImpl(map.list(name));
  }

  public boolean isMapWriter() {
    return map != null;
  }

  public boolean isListWriter() {
    return list != null;
  }

  public VarCharWriter varChar(final String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.varChar(name, dataMode) : list.varChar();
  }

  public IntWriter integer(final String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.integer(name, dataMode) : list.integer();
  }

  public BigIntWriter bigInt(final String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.bigInt(name, dataMode) : list.bigInt();
  }

  public Float4Writer float4(final String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.float4(name, dataMode) : list.float4();
  }

  public Float8Writer float8(final String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.float8(name, dataMode) : list.float8();
  }

  public BitWriter bit(final String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.bit(name, dataMode) : list.bit();
  }

  /**
   * {@inheritDoc}
   */
  @Deprecated
  public VarBinaryWriter binary(final String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.varBinary(name, dataMode) : list.varBinary();
  }

  @Override
  public TinyIntWriter tinyInt(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.tinyInt(name, dataMode) : list.tinyInt();
  }

  @Override
  public SmallIntWriter smallInt(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.smallInt(name, dataMode) : list.smallInt();
  }

  @Override
  public DateWriter date(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.date(name, dataMode) : list.date();
  }

  @Override
  public TimeWriter time(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.time(name, dataMode) : list.time();
  }

  @Override
  public TimeStampWriter timeStamp(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.timeStamp(name, dataMode) : list.timeStamp();
  }

  @Override
  public VarBinaryWriter varBinary(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.varBinary(name, dataMode) : list.varBinary();
  }

  @Override
  public Var16CharWriter var16Char(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.var16Char(name, dataMode) : list.var16Char();
  }

  @Override
  public UInt1Writer uInt1(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.uInt1(name, dataMode) : list.uInt1();
  }

  @Override
  public UInt2Writer uInt2(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.uInt2(name, dataMode) : list.uInt2();
  }

  @Override
  public UInt4Writer uInt4(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.uInt4(name, dataMode) : list.uInt4();
  }

  @Override
  public UInt8Writer uInt8(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.uInt8(name, dataMode) : list.uInt8();
  }

  @Override
  public IntervalYearWriter intervalYear(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.intervalYear(name, dataMode) : list.intervalYear();
  }

  @Override
  public IntervalDayWriter intervalDay(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.intervalDay(name, dataMode) : list.intervalDay();
  }

  @Override
  public IntervalWriter interval(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.interval(name, dataMode) : list.interval();
  }

  @Override
  public Decimal9Writer decimal9(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.decimal9(name, dataMode) : list.decimal9();
  }

  @Override
  public Decimal18Writer decimal18(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.decimal18(name, dataMode) : list.decimal18();
  }

  @Override
  public Decimal28DenseWriter decimal28Dense(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.decimal28Dense(name, dataMode) : list.decimal28Dense();
  }

  @Override
  public Decimal38DenseWriter decimal38Dense(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.decimal38Dense(name, dataMode) : list.decimal38Dense();
  }

  @Override
  public Decimal38SparseWriter decimal38Sparse(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.decimal38Sparse(name, dataMode) : list.decimal38Sparse();
  }

  @Override
  public Decimal28SparseWriter decimal28Sparse(String name, TypeProtos.DataMode dataMode) {
    return (map != null) ? map.decimal28Sparse(name, dataMode) : list.decimal28Sparse();
  }

}
