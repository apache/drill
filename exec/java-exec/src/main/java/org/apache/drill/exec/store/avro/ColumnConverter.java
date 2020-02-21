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
package org.apache.drill.exec.store.avro;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.DictWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * Converts and sets given value into the specific column writer.
 */
public interface ColumnConverter {

  void convert(Object value);

  /**
   * Does nothing, is used when column is not projected to avoid unnecessary
   * column values conversions and writes.
   */
  class DummyColumnConverter implements ColumnConverter {

    public static final DummyColumnConverter INSTANCE = new DummyColumnConverter();

    @Override
    public void convert(Object value) {
      // do nothing
    }
  }

  /**
   * Converts and writes scalar values using provided {@link #valueConverter}.
   * {@link #valueConverter} has different implementation depending
   * on the scalar value type.
   */
  class ScalarColumnConverter implements ColumnConverter {

    private final Consumer<Object> valueConverter;

    public ScalarColumnConverter(Consumer<Object> valueConverter) {
      this.valueConverter = valueConverter;
    }

    @Override
    public void convert(Object value) {
      if (value == null) {
        return;
      }

      valueConverter.accept(value);
    }
  }

  /**
   * Converts and writes array values using {@link #valueConverter}
   * into {@link #arrayWriter}.
   */
  class ArrayColumnConverter implements ColumnConverter {

    private final ArrayWriter arrayWriter;
    private final ColumnConverter valueConverter;

    public ArrayColumnConverter(ArrayWriter arrayWriter, ColumnConverter valueConverter) {
      this.arrayWriter = arrayWriter;
      this.valueConverter = valueConverter;
    }

    @Override
    public void convert(Object value) {
      if (value == null || !arrayWriter.isProjected()) {
        return;
      }

      GenericArray<?> array = (GenericArray<?>) value;
      array.forEach(arrayValue -> {
        valueConverter.convert(arrayValue);
        arrayWriter.save();
      });
    }
  }

  /**
   * Converts and writes all map children using provided {@link #converters}.
   * If {@link #converters} are empty, generates their converters based on
   * {@link GenericRecord} schema.
   */
  class MapColumnConverter implements ColumnConverter {

    private final ColumnConverterFactory factory;
    private final TupleMetadata providedSchema;
    private final TupleWriter tupleWriter;
    private final List<ColumnConverter> converters;

    public MapColumnConverter(ColumnConverterFactory factory,
        TupleMetadata providedSchema,
        TupleWriter tupleWriter, List<ColumnConverter> converters) {
      this.factory = factory;
      this.providedSchema = providedSchema;
      this.tupleWriter = tupleWriter;
      this.converters = new ArrayList<>(converters);
    }

    @Override
    public void convert(Object value) {
      if (value == null) {
        return;
      }

      GenericRecord genericRecord = (GenericRecord) value;

      if (converters.isEmpty()) {
        factory.buildMapMembers(genericRecord, providedSchema, tupleWriter, converters);
      }

      IntStream.range(0, converters.size())
        .forEach(i -> converters.get(i).convert(genericRecord.get(i)));
    }
  }

  /**
   * Converts and writes dict values using provided key / value converters.
   */
  class DictColumnConverter implements ColumnConverter {

    private final DictWriter dictWriter;
    private final ColumnConverter keyConverter;
    private final ColumnConverter valueConverter;

    public DictColumnConverter(DictWriter dictWriter, ColumnConverter keyConverter, ColumnConverter valueConverter) {
      this.dictWriter = dictWriter;
      this.keyConverter = keyConverter;
      this.valueConverter = valueConverter;
    }

    @Override
    public void convert(Object value) {
      if (value == null) {
        return;
      }

      @SuppressWarnings("unchecked") Map<Object, Object> map = (Map<Object, Object>) value;
      map.forEach((key, val) -> {
        keyConverter.convert(key);
        valueConverter.convert(val);
        dictWriter.save();
      });
    }
  }
}
