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
package org.apache.drill.exec.store.avro;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for generating Avro test data.
 */
public class AvroTestUtil {

  public static final int RECORD_COUNT = 10;

  public static String generateSimplePrimitiveSchema_NoNullValues() throws Exception {

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_long").type().longType().noDefault()
            .name("d_float").type().floatType().noDefault()
            .name("e_double").type().doubleType().noDefault()
            .name("f_bytes").type().bytesType().noDefault()
            .name("g_null").type().nullType().noDefault()
            .name("h_boolean").type().booleanType().noDefault()
            .endRecord();

    final File file = File.createTempFile("avro-primitive-test", ".avro");
    file.deleteOnExit();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    try {
      writer.create(schema, file);

      ByteBuffer bb = ByteBuffer.allocate(1);
      bb.put(0, (byte) 1);

      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_int", i);
        record.put("c_long", (long) i);
        record.put("d_float", (float) i);
        record.put("e_double", (double) i);
        record.put("f_bytes", bb);
        record.put("g_null", null);
        record.put("h_boolean", (i % 2 == 0));
        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }

  public static String generateUnionSchema_WithNullValues() throws Exception {

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_long").type().longType().noDefault()
            .name("d_float").type().floatType().noDefault()
            .name("e_double").type().doubleType().noDefault()
            .name("f_bytes").type().bytesType().noDefault()
            .name("g_null").type().nullType().noDefault()
            .name("h_boolean").type().booleanType().noDefault()
            .name("i_union").type().optional().doubleType()
            .endRecord();

    final File file = File.createTempFile("avro-primitive-test", ".avro");
    file.deleteOnExit();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    try {
      writer.create(schema, file);

      ByteBuffer bb = ByteBuffer.allocate(1);
      bb.put(0, (byte) 1);

      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_int", i);
        record.put("c_long", (long) i);
        record.put("d_float", (float) i);
        record.put("e_double", (double) i);
        record.put("f_bytes", bb);
        record.put("g_null", null);
        record.put("h_boolean", (i % 2 == 0));
        record.put("i_union", (i % 2 == 0 ? (double) i : null));
        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }

  public static String generateUnionSchema_WithNonNullValues() throws Exception {

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_long").type().longType().noDefault()
            .name("d_float").type().floatType().noDefault()
            .name("e_double").type().doubleType().noDefault()
            .name("f_bytes").type().bytesType().noDefault()
            .name("g_null").type().nullType().noDefault()
            .name("h_boolean").type().booleanType().noDefault()
            .name("i_union").type().unionOf().doubleType().and().longType().endUnion().noDefault()
            .endRecord();

    final File file = File.createTempFile("avro-primitive-test", ".avro");
    file.deleteOnExit();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    try {
      writer.create(schema, file);

      ByteBuffer bb = ByteBuffer.allocate(1);
      bb.put(0, (byte) 1);

      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_int", i);
        record.put("c_long", (long) i);
        record.put("d_float", (float) i);
        record.put("e_double", (double) i);
        record.put("f_bytes", bb);
        record.put("g_null", null);
        record.put("h_boolean", (i % 2 == 0));
        record.put("i_union", (i % 2 == 0 ? (double) i : (long) i));
        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }

  public static String generateSimpleEnumSchema_NoNullValues() throws Exception {

    final String[] symbols = { "E_SYM_A", "E_SYM_B", "E_SYM_C", "E_SYM_D" };

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_enum").type().enumeration("my_enum").symbols(symbols).noDefault()
            .endRecord();

    final File file = File.createTempFile("avro-primitive-test", ".avro");
    file.deleteOnExit();

    final Schema enumSchema = schema.getField("b_enum").schema();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));

    try {
      writer.create(schema, file);

      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        final GenericData.EnumSymbol symbol =
                new GenericData.EnumSymbol(enumSchema, symbols[(i + symbols.length) % symbols.length]);
        record.put("b_enum", symbol);
        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }

  public static String generateSimpleArraySchema_NoNullValues() throws Exception {

    final File file = File.createTempFile("avro-array-test", ".avro");
    file.deleteOnExit();

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_string_array").type().array().items().stringType().noDefault()
            .name("d_int_array").type().array().items().intType().noDefault()
            .name("e_float_array").type().array().items().floatType().noDefault()
            .endRecord();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    try {
      writer.create(schema, file);

      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        GenericArray array = new GenericData.Array<String>(RECORD_COUNT, schema.getField("c_string_array").schema());
        for (int j = 0; j < RECORD_COUNT; j++) {
          array.add(j, "c_string_array_" + i + "_" + j);
        }
        record.put("c_string_array", array);

        array = new GenericData.Array<String>(RECORD_COUNT, schema.getField("d_int_array").schema());
        for (int j = 0; j < RECORD_COUNT; j++) {
          array.add(j, i * j);
        }
        record.put("d_int_array", array);

        array = new GenericData.Array<String>(RECORD_COUNT, schema.getField("e_float_array").schema());
        for (int j = 0; j < RECORD_COUNT; j++) {
          array.add(j, (float) (i * j));
        }
        record.put("e_float_array", array);

        writer.append(record);
      }

    } finally {
      writer.close();
    }
    return file.getAbsolutePath();
  }

  public static String generateSimpleNestedSchema_NoNullValues() throws Exception {

    final File file = File.createTempFile("avro-nested-test", ".avro");
    file.deleteOnExit();

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_record").type().record("my_record_1")
              .namespace("foo.blah.org")
              .fields()
              .name("nested_1_string").type().stringType().noDefault()
              .name("nested_1_int").type().intType().noDefault()
              .endRecord()
            .noDefault()
            .endRecord();

    final Schema nestedSchema = schema.getField("c_record").schema();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    writer.create(schema, file);

    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        final GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
        nestedRecord.put("nested_1_string", "nested_1_string_" +  i);
        nestedRecord.put("nested_1_int", i * i);

        record.put("c_record", nestedRecord);
        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }

  public static String generateUnionNestedArraySchema_withNullValues() throws Exception {

    final File file = File.createTempFile("avro-nested-test", ".avro");
    file.deleteOnExit();

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_array").type().optional().array().items().record("my_record_1")
            .namespace("foo.blah.org").fields()
            .name("nested_1_string").type().optional().stringType()
            .name("nested_1_int").type().optional().intType()
            .endRecord()
            .endRecord();

    final Schema nestedSchema = schema.getField("c_array").schema();
    final Schema arraySchema = nestedSchema.getTypes().get(1);
    final Schema itemSchema = arraySchema.getElementType();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    writer.create(schema, file);

    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        if (i % 2 == 0) {
          GenericArray array = new GenericData.Array<String>(1, arraySchema);
          final GenericRecord nestedRecord = new GenericData.Record(itemSchema);
          nestedRecord.put("nested_1_string", "nested_1_string_" +  i);
          nestedRecord.put("nested_1_int", i * i);
          array.add(nestedRecord);
          record.put("c_array", array);
        }
        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }

  public static String generateMapSchema_withNullValues() throws Exception {

    final File file = File.createTempFile("avro-nested-test", ".avro");
    file.deleteOnExit();

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_map").type().optional().map().values(Schema.create(Type.STRING))
            .endRecord();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    writer.create(schema, file);

    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        if (i % 2 == 0) {
          Map<String, String> strMap = new HashMap<>();
          strMap.put("key1", "nested_1_string_" +  i);
          strMap.put("key2", "nested_1_string_" +  (i + 1 ));
          record.put("c_map", strMap);
        }
        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }

  public static String generateMapSchemaComplex_withNullValues() throws Exception {

    final File file = File.createTempFile("avro-nested-test", ".avro");
    file.deleteOnExit();

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_map").type().optional().map().values(Schema.create(Type.STRING))
            .name("d_map").type().optional().map().values(Schema.createArray(Schema.create(Type.DOUBLE)))
            .endRecord();

    final Schema arrayMapSchema = schema.getField("d_map").schema();
    final Schema arrayItemSchema = arrayMapSchema.getTypes().get(1).getValueType();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    writer.create(schema, file);

    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        if (i % 2 == 0) {
          Map<String, String> c_map = new HashMap<>();
          c_map.put("key1", "nested_1_string_" +  i);
          c_map.put("key2", "nested_1_string_" +  (i + 1 ));
          record.put("c_map", c_map);
        } else {
          Map<String, GenericArray> d_map = new HashMap<>();
          GenericArray array = new GenericData.Array<String>(RECORD_COUNT, arrayItemSchema);
          for (int j = 0; j < RECORD_COUNT; j++) {
            array.add((double)j);
          }
          d_map.put("key1", array);
          d_map.put("key2", array);

          record.put("d_map", d_map);
        }
        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }

  public static String generateUnionNestedSchema_withNullValues() throws Exception {

    final File file = File.createTempFile("avro-nested-test", ".avro");
    file.deleteOnExit();

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_record").type().optional().record("my_record_1")
            .namespace("foo.blah.org").fields()
            .name("nested_1_string").type().optional().stringType()
            .name("nested_1_int").type().optional().intType()
            .endRecord()
            .endRecord();

    final Schema nestedSchema = schema.getField("c_record").schema();
    final Schema optionalSchema = nestedSchema.getTypes().get(1);

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    writer.create(schema, file);

    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        if (i % 2 == 0) {
          final GenericRecord nestedRecord = new GenericData.Record(optionalSchema);
          nestedRecord.put("nested_1_string", "nested_1_string_" +  i);
          nestedRecord.put("nested_1_int", i * i);
          record.put("c_record", nestedRecord);
        }
        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }

  public static String generateDoubleNestedSchema_NoNullValues() throws Exception {

    final File file = File.createTempFile("avro-double-nested-test", ".avro");
    file.deleteOnExit();

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_record").type().record("my_record_1")
              .namespace("foo.blah.org")
              .fields()
              .name("nested_1_string").type().stringType().noDefault()
              .name("nested_1_int").type().intType().noDefault()
              .name("nested_1_record").type().record("my_double_nested_record_1")
                .namespace("foo.blah.org.rot")
                .fields()
                .name("double_nested_1_string").type().stringType().noDefault()
                .name("double_nested_1_int").type().intType().noDefault()
                .endRecord()
                .noDefault()
              .endRecord()
              .noDefault()
            .endRecord();

    final Schema nestedSchema = schema.getField("c_record").schema();
    final Schema doubleNestedSchema = nestedSchema.getField("nested_1_record").schema();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    writer.create(schema, file);

    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        final GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
        nestedRecord.put("nested_1_string", "nested_1_string_" +  i);
        nestedRecord.put("nested_1_int", i * i);

        final GenericRecord doubleNestedRecord = new GenericData.Record(doubleNestedSchema);
        doubleNestedRecord.put("double_nested_1_string", "double_nested_1_string_" + i + "_" + i);
        doubleNestedRecord.put("double_nested_1_int", i * i * i);

        nestedRecord.put("nested_1_record", doubleNestedRecord);
        record.put("c_record", nestedRecord);

        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }

  public static String generateStringAndUtf8Data() throws Exception {

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringBuilder().prop("avro.java.string", "String").endString().noDefault()
            .name("b_utf8").type().stringType().noDefault()
            .endRecord();

    final File file = File.createTempFile("avro-primitive-test", ".avro");
    file.deleteOnExit();

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter(schema));
    try {
      writer.create(schema, file);

      ByteBuffer bb = ByteBuffer.allocate(1);
      bb.put(0, (byte) 1);

      for (int i = 0; i < RECORD_COUNT; i++) {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("a_string", "a_" + i);
        record.put("b_utf8", "b_" + i);
        writer.append(record);
      }
    } finally {
      writer.close();
    }

    return file.getAbsolutePath();
  }
}
