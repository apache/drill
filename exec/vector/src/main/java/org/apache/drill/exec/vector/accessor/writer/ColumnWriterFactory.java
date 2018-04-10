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
package org.apache.drill.exec.vector.accessor.writer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnAccessors;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter.ArrayObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter.ScalarObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter.TupleObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter.ArrayMapWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter.DummyArrayMapWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter.DummyMapWriter;
import org.apache.drill.exec.vector.accessor.writer.MapWriter.SingleMapWriter;
import org.apache.drill.exec.vector.accessor.writer.dummy.DummyArrayWriter;
import org.apache.drill.exec.vector.accessor.writer.dummy.DummyScalarWriter;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Gather generated writer classes into a set of class tables to allow rapid
 * run-time creation of writers. Builds the writer and its object writer
 * wrapper which binds the vector to the writer.
 */

@SuppressWarnings("unchecked")
public class ColumnWriterFactory {

  private static final int typeCount = MinorType.values().length;
  private static final Class<? extends BaseScalarWriter> requiredWriters[] = new Class[typeCount];

  static {
    ColumnAccessors.defineRequiredWriters(requiredWriters);
  }

  public static AbstractObjectWriter buildColumnWriter(ColumnMetadata schema, ValueVector vector) {
    if (vector == null) {
      return buildDummyColumnWriter(schema);
    }

    // Build a writer for a materialized column.

    assert schema.type() == vector.getField().getType().getMinorType();
    assert schema.mode() == vector.getField().getType().getMode();

    switch (schema.type()) {
    case GENERIC_OBJECT:
    case LATE:
    case NULL:
    case LIST:
    case MAP:
      throw new UnsupportedOperationException(schema.type().toString());
    default:
      switch (schema.mode()) {
      case OPTIONAL:
        NullableVector nullableVector = (NullableVector) vector;
        return NullableScalarWriter.build(schema, nullableVector,
                newWriter(nullableVector.getValuesVector()));
      case REQUIRED:
        return new ScalarObjectWriter(schema, newWriter(vector));
      case REPEATED:
        RepeatedValueVector repeatedVector = (RepeatedValueVector) vector;
        return ScalarArrayWriter.build(schema, repeatedVector,
                newWriter(repeatedVector.getDataVector()));
      default:
        throw new UnsupportedOperationException(schema.mode().toString());
      }
    }
  }

  /**
   * Build a writer for a non-projected column.
   * @param schema schema of the column
   * @return a "dummy" writer for the column
   */

  public static AbstractObjectWriter buildDummyColumnWriter(ColumnMetadata schema) {
    switch (schema.type()) {
    case GENERIC_OBJECT:
    case LATE:
    case NULL:
    case LIST:
    case MAP:
      throw new UnsupportedOperationException(schema.type().toString());
    default:
      ScalarObjectWriter scalarWriter = new ScalarObjectWriter(schema,
          new DummyScalarWriter());
      switch (schema.mode()) {
      case OPTIONAL:
      case REQUIRED:
        return scalarWriter;
      case REPEATED:
        return new ArrayObjectWriter(schema,
            new DummyArrayWriter(
              scalarWriter));
      default:
        throw new UnsupportedOperationException(schema.mode().toString());
      }
    }
  }

  public static TupleObjectWriter buildMap(ColumnMetadata schema, MapVector vector,
                                        List<AbstractObjectWriter> writers) {
    MapWriter mapWriter;
    if (schema.isProjected()) {
      mapWriter = new SingleMapWriter(schema, vector, writers);
    } else {
      mapWriter = new DummyMapWriter(schema, writers);
    }
    return new TupleObjectWriter(schema, mapWriter);
  }

  public static ArrayObjectWriter buildMapArray(ColumnMetadata schema,
                                        UInt4Vector offsetVector,
                                        List<AbstractObjectWriter> writers) {
    MapWriter mapWriter;
    if (schema.isProjected()) {
      mapWriter = new ArrayMapWriter(schema, writers);
    } else {
      mapWriter = new DummyArrayMapWriter(schema, writers);
    }
    TupleObjectWriter mapArray = new TupleObjectWriter(schema, mapWriter);
    AbstractArrayWriter arrayWriter;
    if (schema.isProjected()) {
      arrayWriter = new ObjectArrayWriter(
          offsetVector,
          mapArray);
    } else  {
      arrayWriter = new DummyArrayWriter(mapArray);
    }
    return new ArrayObjectWriter(schema, arrayWriter);
  }

  public static AbstractObjectWriter buildMapWriter(ColumnMetadata schema,
      AbstractMapVector vector,
      List<AbstractObjectWriter> writers) {
    assert (vector != null) == schema.isProjected();
    if (! schema.isArray()) {
      return buildMap(schema, (MapVector) vector, writers);
    } else if (vector == null) {
      return buildMapArray(schema,
          null, writers);
    } else {
      return buildMapArray(schema,
          ((RepeatedMapVector) vector).getOffsetVector(),
          writers);
    }
  }

  public static AbstractObjectWriter buildMapWriter(ColumnMetadata schema, AbstractMapVector vector) {
    assert schema.mapSchema().size() == 0;
    return buildMapWriter(schema, vector, new ArrayList<AbstractObjectWriter>());
  }

  public static BaseScalarWriter newWriter(ValueVector vector) {
    MajorType major = vector.getField().getType();
    MinorType type = major.getMinorType();
    try {
      Class<? extends BaseScalarWriter> accessorClass = requiredWriters[type.ordinal()];
      if (accessorClass == null) {
        throw new UnsupportedOperationException(type.toString());
      }
      Constructor<? extends BaseScalarWriter> ctor = accessorClass.getConstructor(ValueVector.class);
      return ctor.newInstance(vector);
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
             SecurityException | IllegalArgumentException | InvocationTargetException e) {
      throw new IllegalStateException(e);
    }
  }
}
