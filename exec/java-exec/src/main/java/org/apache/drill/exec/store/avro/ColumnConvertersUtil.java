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

import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.avro.ColumnConverter.ArrayColumnConverter;
import org.apache.drill.exec.store.avro.ColumnConverter.DictColumnConverter;
import org.apache.drill.exec.store.avro.ColumnConverter.DummyColumnConverter;
import org.apache.drill.exec.store.avro.ColumnConverter.MapColumnConverter;
import org.apache.drill.exec.store.avro.ColumnConverter.ScalarColumnConverter;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.DictWriter;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.complex.DictVector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ColumnConvertersUtil {

  /**
   * Based on given converted Avro schema and current row writer generates list of
   * column converters based on column type.
   *
   * @param schema converted Avro schema
   * @param rowWriter current row writer
   * @return list of column converters
   */
  public static List<ColumnConverter> initConverters(TupleMetadata schema, RowSetLoader rowWriter) {
    return IntStream.range(0, schema.size())
      .mapToObj(i -> getConverter(schema.metadata(i), rowWriter.column(i)))
      .collect(Collectors.toList());
  }

  /**
   * Based on column type, creates corresponding column converter
   * which holds conversion logic and appropriate writer to set converted data into.
   * For columns which are not projected, {@link DummyColumnConverter} is used.
   *
   * @param metadata column metadata
   * @param writer column writer
   * @return column converter
   */
  public static ColumnConverter getConverter(ColumnMetadata metadata, ObjectWriter writer) {
    if (!writer.isProjected()) {
      return DummyColumnConverter.INSTANCE;
    }

    if (metadata.isArray()) {
      return getArrayConverter(metadata, writer.array());
    }

    if (metadata.isMap()) {
      return getMapConverter(metadata.tupleSchema(), writer.tuple());
    }

    if (metadata.isDict()) {
      return getDictConverter(metadata.tupleSchema(), writer.dict());
    }

    return getScalarConverter(writer.scalar());
  }

  private static ColumnConverter getArrayConverter(ColumnMetadata metadata, ArrayWriter arrayWriter) {
    ObjectWriter valueWriter = arrayWriter.entry();
    ColumnConverter valueConverter;
    if (metadata.isMap()) {
      valueConverter = getMapConverter(metadata.tupleSchema(), valueWriter.tuple());
    } else if (metadata.isDict()) {
      valueConverter = getDictConverter(metadata.tupleSchema(), valueWriter.dict());
    } else if (metadata.isMultiList()) {
      valueConverter = getConverter(metadata.childSchema(), valueWriter);
    } else {
      valueConverter = getScalarConverter(valueWriter.scalar());
    }
    return new ArrayColumnConverter(arrayWriter, valueConverter);
  }

  private static ColumnConverter getMapConverter(TupleMetadata metadata, TupleWriter tupleWriter) {
    List<ColumnConverter> converters = IntStream.range(0, metadata.size())
      .mapToObj(i -> getConverter(metadata.metadata(i), tupleWriter.column(i)))
      .collect(Collectors.toList());
    return new MapColumnConverter(tupleWriter, converters);
  }

  private static ColumnConverter getDictConverter(TupleMetadata metadata, DictWriter dictWriter) {
    ColumnConverter keyConverter = getScalarConverter(dictWriter.keyWriter());
    ColumnConverter valueConverter = getConverter(metadata.metadata(DictVector.FIELD_VALUE_NAME), dictWriter.valueWriter());
    return new DictColumnConverter(dictWriter, keyConverter, valueConverter);
  }

  private static ColumnConverter getScalarConverter(ScalarWriter scalarWriter) {
    return ScalarColumnConverter.init(scalarWriter);
  }
}
