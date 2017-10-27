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
import org.apache.drill.exec.vector.accessor.ColumnAccessors;
import org.apache.drill.exec.vector.accessor.impl.AbstractArrayReader.ArrayColumnReader;
import org.apache.drill.exec.vector.accessor.impl.AbstractArrayWriter.ArrayColumnWriter;

/**
 * Gather generated accessor classes into a set of class
 * tables to allow rapid run-time creation of accessors.
 * The caller is responsible for binding the accessor to
 * a vector and a row index.
 */

public class ColumnAccessorFactory {

  private static Class<? extends AbstractColumnWriter> columnWriters[][] = buildColumnWriters();
  private static Class<? extends AbstractColumnReader> columnReaders[][] = buildColumnReaders();
  private static Class<? extends AbstractArrayWriter> arrayWriters[] = buildArrayWriters();
  private static Class<? extends AbstractArrayReader> arrayReaders[] = buildArrayReaders();

  @SuppressWarnings("unchecked")
  private static Class<? extends AbstractColumnWriter>[][] buildColumnWriters() {
    int typeCount = MinorType.values().length;
    int modeCount = DataMode.values().length;
    Class<? extends AbstractColumnWriter> writers[][] = new Class[typeCount][];
    for (int i = 0; i < typeCount; i++) {
      writers[i] = new Class[modeCount];
    }

    ColumnAccessors.defineWriters(writers);
    return writers;
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends AbstractColumnReader>[][] buildColumnReaders() {
    int typeCount = MinorType.values().length;
    int modeCount = DataMode.values().length;
    Class<? extends AbstractColumnReader> readers[][] = new Class[typeCount][];
    for (int i = 0; i < typeCount; i++) {
      readers[i] = new Class[modeCount];
    }

    ColumnAccessors.defineReaders(readers);
    return readers;
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends AbstractArrayWriter>[] buildArrayWriters() {
    int typeCount = MinorType.values().length;
    Class<? extends AbstractArrayWriter> writers[] = new Class[typeCount];
    ColumnAccessors.defineArrayWriters(writers);
    return writers;
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends AbstractArrayReader>[] buildArrayReaders() {
    int typeCount = MinorType.values().length;
    Class<? extends AbstractArrayReader> readers[] = new Class[typeCount];
    ColumnAccessors.defineArrayReaders(readers);
    return readers;
  }

  public static AbstractColumnWriter newWriter(MajorType type) {
    try {
      if (type.getMode() == DataMode.REPEATED) {
        Class<? extends AbstractArrayWriter> writerClass = arrayWriters[type.getMinorType().ordinal()];
        if (writerClass == null) {
          throw new UnsupportedOperationException();
        }
        return new ArrayColumnWriter(writerClass.newInstance());
      } else {
        Class<? extends AbstractColumnWriter> writerClass = columnWriters[type.getMinorType().ordinal()][type.getMode().ordinal()];
        if (writerClass == null) {
          throw new UnsupportedOperationException();
        }
        return writerClass.newInstance();
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  public static AbstractColumnReader newReader(MajorType type) {
    try {
      if (type.getMode() == DataMode.REPEATED) {
        Class<? extends AbstractArrayReader> readerClass = arrayReaders[type.getMinorType().ordinal()];
        if (readerClass == null) {
          throw new UnsupportedOperationException();
        }
        return new ArrayColumnReader(readerClass.newInstance());
      } else {
        Class<? extends AbstractColumnReader> readerClass = columnReaders[type.getMinorType().ordinal()][type.getMode().ordinal()];
        if (readerClass == null) {
          throw new UnsupportedOperationException();
        }
        return readerClass.newInstance();
      }
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }
}
