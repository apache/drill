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
package org.apache.drill.exec.vector.accessor.reader;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.VariantMetadata;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.exec.vector.accessor.ValueType;
import org.apache.drill.exec.vector.accessor.VariantReader;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.Period;

import java.math.BigDecimal;

/**
 * Dummy reader which returns {@code null} for scalar types and itself for complex types.
 */
public class NullReader implements ScalarReader, ArrayReader, TupleReader, VariantReader, ObjectReader {

  private static final NullReader INSTANCE = new NullReader();

  public static NullReader instance() {
    return INSTANCE;
  }

  @Override
  public int size() {
    notSupported();
    return 0;
  }

  @Override
  public ObjectType entryType() {
    notSupported();
    return null;
  }

  @Override
  public ObjectReader entry() {
    notSupported();
    return null;
  }

  @Override
  public ScalarReader scalar() {
    return this;
  }

  @Override
  public TupleReader tuple() {
    return this;
  }

  @Override
  public ArrayReader array() {
    return this;
  }

  @Override
  public VariantReader variant() {
    return this;
  }

  @Override
  public void setPosn(int index) {
    notSupported();
  }

  @Override
  public void rewind() {
    notSupported();
  }

  @Override
  public boolean next() {
    return false;
  }

  @Override
  public ValueType valueType() {
    notSupported();
    return null;
  }

  @Override
  public ValueType extendedType() {
    notSupported();
    return null;
  }

  @Override
  public int getInt() {
    notSupported();
    return 0;
  }

  @Override
  public boolean getBoolean() {
    notSupported();
    return false;
  }

  @Override
  public long getLong() {
    notSupported();
    return 0;
  }

  @Override
  public double getDouble() {
    notSupported();
    return 0;
  }

  @Override
  public String getString() {
    return null;
  }

  @Override
  public byte[] getBytes() {
    return null;
  }

  @Override
  public BigDecimal getDecimal() {
    return null;
  }

  @Override
  public Period getPeriod() {
    return null;
  }

  @Override
  public LocalDate getDate() {
    return null;
  }

  @Override
  public LocalTime getTime() {
    return null;
  }

  @Override
  public Instant getTimestamp() {
    return null;
  }

  @Override
  public Object getValue() {
    return null;
  }

  @Override
  public TupleMetadata tupleSchema() {
    return null;
  }

  @Override
  public int columnCount() {
    notSupported();
    return 0;
  }

  @Override
  public ObjectReader column(int colIndex) {
    notSupported();
    return null;
  }

  @Override
  public ObjectReader column(String colName) {
    notSupported();
    return null;
  }

  @Override
  public ObjectType type(int colIndex) {
    notSupported();
    return null;
  }

  @Override
  public ObjectType type(String colName) {
    notSupported();
    return null;
  }

  @Override
  public ScalarReader scalar(int colIndex) {
    notSupported();
    return null;
  }

  @Override
  public ScalarReader scalar(String colName) {
    notSupported();
    return null;
  }

  @Override
  public TupleReader tuple(int colIndex) {
    notSupported();
    return null;
  }

  @Override
  public TupleReader tuple(String colName) {
    notSupported();
    return null;
  }

  @Override
  public ArrayReader array(int colIndex) {
    notSupported();
    return null;
  }

  @Override
  public ArrayReader array(String colName) {
    notSupported();
    return null;
  }

  @Override
  public VariantReader variant(int colIndex) {
    notSupported();
    return null;
  }

  @Override
  public VariantReader variant(String colName) {
    notSupported();
    return null;
  }

  @Override
  public VariantMetadata variantSchema() {
    notSupported();
    return null;
  }

  @Override
  public boolean hasType(TypeProtos.MinorType type) {
    notSupported();
    return false;
  }

  @Override
  public ObjectReader member(TypeProtos.MinorType type) {
    notSupported();
    return null;
  }

  @Override
  public ScalarReader scalar(TypeProtos.MinorType type) {
    notSupported();
    return null;
  }

  @Override
  public TypeProtos.MinorType dataType() {
    notSupported();
    return null;
  }

  @Override
  public ObjectReader member() {
    notSupported();
    return null;
  }

  @Override
  public ColumnMetadata schema() {
    notSupported();
    return null;
  }

  @Override
  public ObjectType type() {
    notSupported();
    return null;
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  public Object getObject() {
    return null;
  }

  @Override
  public String getAsString() {
    return "null";
  }

  private void notSupported() {
    throw new UnsupportedOperationException();
  }
}
