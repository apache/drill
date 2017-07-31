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
package org.apache.drill.exec.vector.accessor.impl;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnAccessors;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.BaseElementReader;
import org.apache.drill.exec.vector.accessor.reader.BaseScalarReader;
import org.apache.drill.exec.vector.accessor.reader.ScalarArrayReader;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.BaseScalarWriter;
import org.apache.drill.exec.vector.accessor.writer.NullableScalarWriter;
import org.apache.drill.exec.vector.accessor.writer.ScalarArrayWriter;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Gather generated accessor classes into a set of class tables to allow rapid
 * run-time creation of accessors. Builds the accessor and its object reader/writer
 * wrapper which binds the vector to the accessor.
 */

@SuppressWarnings("unchecked")
public class ColumnAccessorFactory {

  private static final int typeCount = MinorType.values().length;
  private static final Class<? extends BaseScalarReader> requiredReaders[] = new Class[typeCount];
  private static final Class<? extends BaseScalarReader> nullableReaders[] = new Class[typeCount];
  private static final Class<? extends BaseElementReader> elementReaders[] = new Class[typeCount];
  private static final Class<? extends BaseScalarWriter> requiredWriters[] = new Class[typeCount];

  static {
    ColumnAccessors.defineRequiredReaders(requiredReaders);
    ColumnAccessors.defineNullableReaders(nullableReaders);
    ColumnAccessors.defineArrayReaders(elementReaders);
    ColumnAccessors.defineRequiredWriters(requiredWriters);
  }

  public static AbstractObjectWriter buildColumnWriter(ValueVector vector) {
    MajorType major = vector.getField().getType();
    MinorType type = major.getMinorType();
    DataMode mode = major.getMode();

    switch (type) {
    case GENERIC_OBJECT:
    case LATE:
    case NULL:
    case LIST:
    case MAP:
      throw new UnsupportedOperationException(type.toString());
    default:
      switch (mode) {
      case OPTIONAL:
        return NullableScalarWriter.build(vector, newAccessor(type, requiredWriters));
      case REQUIRED:
        return BaseScalarWriter.build(vector, newAccessor(type, requiredWriters));
      case REPEATED:
        return ScalarArrayWriter.build((RepeatedValueVector) vector, newAccessor(type, requiredWriters));
      default:
        throw new UnsupportedOperationException(mode.toString());
      }
    }
  }

  public static AbstractObjectReader buildColumnReader(ValueVector vector) {
    MajorType major = vector.getField().getType();
    MinorType type = major.getMinorType();
    DataMode mode = major.getMode();

    switch (type) {
    case GENERIC_OBJECT:
    case LATE:
    case NULL:
    case LIST:
    case MAP:
      throw new UnsupportedOperationException(type.toString());
    default:
      switch (mode) {
      case OPTIONAL:
        return BaseScalarReader.build(vector, newAccessor(type, nullableReaders));
      case REQUIRED:
        return BaseScalarReader.build(vector, newAccessor(type, requiredReaders));
      case REPEATED:
        return ScalarArrayReader.build((RepeatedValueVector) vector, newAccessor(type, elementReaders));
      default:
        throw new UnsupportedOperationException(mode.toString());
      }
    }
  }

  public static AbstractObjectReader buildColumnReader(MajorType majorType, VectorAccessor va) {
    MinorType type = majorType.getMinorType();
    DataMode mode = majorType.getMode();

    switch (type) {
    case GENERIC_OBJECT:
    case LATE:
    case NULL:
    case LIST:
    case MAP:
      throw new UnsupportedOperationException(type.toString());
    default:
      switch (mode) {
      case OPTIONAL:
        return BaseScalarReader.build(majorType, va, newAccessor(type, nullableReaders));
      case REQUIRED:
        return BaseScalarReader.build(majorType, va, newAccessor(type, requiredReaders));
      case REPEATED:
        return ScalarArrayReader.build(majorType, va, newAccessor(type, elementReaders));
      default:
        throw new UnsupportedOperationException(mode.toString());
      }
    }
  }

  public static <T> T newAccessor(MinorType type, Class<? extends T> accessors[]) {
    try {
      Class<? extends T> accessorClass = accessors[type.ordinal()];
      if (accessorClass == null) {
        throw new UnsupportedOperationException(type.toString());
      }
      return accessorClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }
}
