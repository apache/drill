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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.util.Text;
import org.apache.drill.test.BaseTestQuery;

/**
 * Utilities for generating Avro test data.
 */
public class AvroTestUtil {

  public static final int RECORD_COUNT = 10_000;
  public static int ARRAY_SIZE = 20;

  /**
   * Class to write records to an Avro file while simultaneously
   * constructing a corresponding list of records in the format taken in
   * by the Drill test builder to describe expected results.
   */
  public static class AvroTestRecordWriter implements Closeable {
    private final List<Map<String, Object>> expectedRecords;
    GenericData.Record currentAvroRecord;
    TreeMap<String, Object> currentExpectedRecord;
    private Schema schema;
    private final DataFileWriter<GenericData.Record> writer;
    private final String filePath;
    private final String fileName;

    private AvroTestRecordWriter(Schema schema, File file) {
      writer = new DataFileWriter<GenericData.Record>(new GenericDatumWriter<GenericData.Record>(schema));
      try {
        writer.create(schema, file);
      } catch (IOException e) {
        throw new RuntimeException("Error creating file in Avro test setup.", e);
      }
      this.schema = schema;
      currentExpectedRecord = new TreeMap<>();
      expectedRecords = new ArrayList<>();
      filePath = file.getAbsolutePath();
      fileName = file.getName();
    }

    public void startRecord() {
      currentAvroRecord = new GenericData.Record(schema);
      currentExpectedRecord = new TreeMap<>();
    }

    public void put(String key, Object value) {
      currentAvroRecord.put(key, value);
      // convert binary values into byte[], the format they will be given
      // in the Drill result set in the test framework
      currentExpectedRecord.put("`" + key + "`", convertAvroValToDrill(value, true));
    }

    // TODO - fix this the test wrapper to prevent the need for this hack
    // to make the root behave differently than nested fields for String vs. Text
    private Object convertAvroValToDrill(Object value, boolean root) {
      if (value instanceof ByteBuffer) {
        ByteBuffer bb = ((ByteBuffer)value);
        byte[] drillVal = new byte[((ByteBuffer)value).remaining()];
        bb.get(drillVal);
        bb.position(0);
        value = drillVal;
      } else if (!root && value instanceof CharSequence) {
        value = new Text(value.toString());
      } else if (value instanceof GenericData.Array) {
        GenericData.Array array = ((GenericData.Array) value);
        final JsonStringArrayList<Object> drillList = new JsonStringArrayList<>();
        for (Object o : array) {
          drillList.add(convertAvroValToDrill(o, false));
        }
        value = drillList;
      } else if (value instanceof GenericData.EnumSymbol) {
        value = value.toString();
      } else if (value instanceof GenericData.Record) {
        GenericData.Record rec = ((GenericData.Record) value);
        final JsonStringHashMap<String, Object> newRecord = new JsonStringHashMap<>();
        for (Schema.Field field : rec.getSchema().getFields()) {
          Object val = rec.get(field.name());
          newRecord.put(field.name(), convertAvroValToDrill(val, false));
        }
        value = newRecord;
      }
      return value;
    }

    public void endRecord() throws IOException {
      writer.append(currentAvroRecord);
      expectedRecords.add(currentExpectedRecord);
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }

    public String getFilePath() {
      return filePath;
    }

    public String getFileName() {
      return fileName;
    }

    public List<Map<String, Object>>getExpectedRecords() {
      return expectedRecords;
    }
  }


  public static AvroTestRecordWriter generateSimplePrimitiveSchema_NoNullValues() throws Exception {
    return generateSimplePrimitiveSchema_NoNullValues(RECORD_COUNT);
  }

  public static AvroTestRecordWriter generateSimplePrimitiveSchema_NoNullValues(int numRecords) throws Exception {
    return generateSimplePrimitiveSchema_NoNullValues(numRecords, "");
  }

  /**
   * Generates Avro table with specified rows number in specified path.
   *
   * @param numRecords rows number in the table
   * @param tablePath  table path
   * @return AvroTestRecordWriter instance
   */
  public static AvroTestRecordWriter generateSimplePrimitiveSchema_NoNullValues(int numRecords, String tablePath)
      throws Exception {
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

    final File file = File.createTempFile("avro-primitive-test", ".avro",
        BaseTestQuery.dirTestWatcher.makeRootSubDir(Paths.get(tablePath)));
    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);

    try {
      ByteBuffer bb = ByteBuffer.allocate(2);
      bb.put(0, (byte) 'a');

      for (int i = 0; i < numRecords; i++) {
        bb.put(1, (byte) ('0' + (i % 10)));
        bb.position(0);
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_int", i);
        record.put("c_long", (long) i);
        record.put("d_float", (float) i);
        record.put("e_double", (double) i);
        record.put("f_bytes", bb);
        record.put("g_null", null);
        record.put("h_boolean", (i % 2 == 0));
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static AvroTestRecordWriter generateUnionSchema_WithNullValues() throws Exception {
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

    final File file = File.createTempFile("avro-primitive-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);

    try {
      ByteBuffer bb = ByteBuffer.allocate(1);
      bb.put(0, (byte) 1);

      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_int", i);
        record.put("c_long", (long) i);
        record.put("d_float", (float) i);
        record.put("e_double", (double) i);
        record.put("f_bytes", bb);
        record.put("g_null", null);
        record.put("h_boolean", (i % 2 == 0));
        record.put("i_union", (i % 2 == 0 ? (double) i : null));
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static AvroTestRecordWriter generateUnionSchema_WithNonNullValues() throws Exception {
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

    final File file = File.createTempFile("avro-primitive-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {

      ByteBuffer bb = ByteBuffer.allocate(1);
      bb.put(0, (byte) 1);

      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_int", i);
        record.put("c_long", (long) i);
        record.put("d_float", (float) i);
        record.put("e_double", (double) i);
        record.put("f_bytes", bb);
        record.put("g_null", null);
        record.put("h_boolean", (i % 2 == 0));
        record.put("i_union", (i % 2 == 0 ? (double) i : (long) i));
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static AvroTestRecordWriter generateSimpleEnumSchema_NoNullValues() throws Exception {
    final String[] symbols = { "E_SYM_A", "E_SYM_B", "E_SYM_C", "E_SYM_D" };

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_enum").type().enumeration("my_enum").symbols(symbols).noDefault()
            .endRecord();

    final File file = File.createTempFile("avro-primitive-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
    final Schema enumSchema = schema.getField("b_enum").schema();

    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {

      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        final GenericData.EnumSymbol symbol =
                new GenericData.EnumSymbol(enumSchema, symbols[(i + symbols.length) % symbols.length]);
        record.put("b_enum", symbol);
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static AvroTestRecordWriter generateSimpleArraySchema_NoNullValues() throws Exception {
    final File file = File.createTempFile("avro-array-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_string_array").type().array().items().stringType().noDefault()
            .name("d_int_array").type().array().items().intType().noDefault()
            .name("e_float_array").type().array().items().floatType().noDefault()
            .endRecord();

    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_int", i);
        {
          GenericArray<String> array = new GenericData.Array<>(ARRAY_SIZE, schema.getField("c_string_array").schema());
          for (int j = 0; j < ARRAY_SIZE; j++) {
            array.add(j, "c_string_array_" + i + "_" + j);
          }
          record.put("c_string_array", array);
        }
        {
          GenericArray<Integer> array = new GenericData.Array<>(ARRAY_SIZE, schema.getField("d_int_array").schema());
          for (int j = 0; j < ARRAY_SIZE; j++) {
            array.add(j, i * j);
          }
          record.put("d_int_array", array);
        }
        {
          GenericArray<Float> array = new GenericData.Array<>(ARRAY_SIZE, schema.getField("e_float_array").schema());
          for (int j = 0; j < ARRAY_SIZE; j++) {
            array.add(j, (float) (i * j));
          }
          record.put("e_float_array", array);
        }
        record.endRecord();
      }

    } finally {
      record.close();
    }
    return record;
  }

  public static AvroTestRecordWriter generateSimpleNestedSchema_NoNullValues() throws Exception {
    final File file = File.createTempFile("avro-nested-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
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

    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        final GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
        nestedRecord.put("nested_1_string", "nested_1_string_" +  i);
        nestedRecord.put("nested_1_int", i * i);

        record.put("c_record", nestedRecord);
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static AvroTestRecordWriter generateUnionNestedArraySchema_withNullValues() throws Exception {
    final File file = File.createTempFile("avro-nested-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
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

    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        if (i % 2 == 0) {
          GenericArray<GenericRecord> array = new GenericData.Array<>(1, arraySchema);
          final GenericRecord nestedRecord = new GenericData.Record(itemSchema);
          nestedRecord.put("nested_1_string", "nested_1_string_" +  i);
          nestedRecord.put("nested_1_int", i * i);
          array.add(nestedRecord);
          record.put("c_array", array);
        }
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static AvroTestRecordWriter generateNestedArraySchema() throws IOException {
    return generateNestedArraySchema(RECORD_COUNT, ARRAY_SIZE);
  }

  public static AvroTestRecordWriter generateNestedArraySchema(int numRecords, int numArrayItems) throws IOException {
    final File file = File.createTempFile("avro-nested-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest").namespace("org.apache.drill.exec.store.avro")
        .fields().name("a_int").type().intType().noDefault().name("b_array").type().array().items()
        .record("my_record_1").namespace("foo.blah.org").fields().name("nested_1_int").type().optional().intType()
        .endRecord().arrayDefault(Collections.emptyList()).endRecord();

    final Schema arraySchema = schema.getField("b_array").schema();
    final Schema itemSchema = arraySchema.getElementType();

    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < numRecords; i++) {
        record.startRecord();
        record.put("a_int", i);
        GenericArray<GenericRecord> array = new GenericData.Array<>(ARRAY_SIZE, arraySchema);

        for (int j = 0; j < numArrayItems; j++) {
          final GenericRecord nestedRecord = new GenericData.Record(itemSchema);
          nestedRecord.put("nested_1_int", j);
          array.add(nestedRecord);
        }
        record.put("b_array", array);
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static AvroTestRecordWriter generateMapSchema_withNullValues() throws Exception {
    final File file = File.createTempFile("avro-nested-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringType().noDefault()
            .name("b_int").type().intType().noDefault()
            .name("c_map").type().optional().map().values(Schema.create(Type.STRING))
            .endRecord();

    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        if (i % 2 == 0) {
          Map<String, String> strMap = new HashMap<>();
          strMap.put("key1", "nested_1_string_" +  i);
          strMap.put("key2", "nested_1_string_" +  (i + 1 ));
          record.put("c_map", strMap);
        }
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static AvroTestRecordWriter generateMapSchemaComplex_withNullValues() throws Exception {
    final File file = File.createTempFile("avro-nested-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
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

    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        if (i % 2 == 0) {
          Map<String, String> c_map = new HashMap<>();
          c_map.put("key1", "nested_1_string_" +  i);
          c_map.put("key2", "nested_1_string_" +  (i + 1 ));
          record.put("c_map", c_map);
        } else {
          Map<String, GenericArray<Double>> d_map = new HashMap<>();
          GenericArray<Double> array = new GenericData.Array<>(ARRAY_SIZE, arrayItemSchema);
          for (int j = 0; j < ARRAY_SIZE; j++) {
            array.add((double)j);
          }
          d_map.put("key1", array);
          d_map.put("key2", array);

          record.put("d_map", d_map);
        }
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static AvroTestRecordWriter generateUnionNestedSchema_withNullValues() throws Exception {
    final File file = File.createTempFile("avro-nested-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
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

    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_int", i);

        if (i % 2 == 0) {
          final GenericRecord nestedRecord = new GenericData.Record(optionalSchema);
          nestedRecord.put("nested_1_string", "nested_1_string_" +  i);
          nestedRecord.put("nested_1_int", i * i);
          record.put("c_record", nestedRecord);
        }
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static AvroTestRecordWriter generateDoubleNestedSchema_NoNullValues() throws Exception {
    final File file = File.createTempFile("avro-double-nested-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
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

    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
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

        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  public static String generateLinkedList() throws Exception {
    final File file = File.createTempFile("avro-linkedlist", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
    final Schema schema = SchemaBuilder.record("LongList")
            .namespace("org.apache.drill.exec.store.avro")
            .aliases("LinkedLongs")
            .fields()
            .name("value").type().optional().longType()
            .name("next").type().optional().type("LongList")
            .endRecord();

    final DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
    writer.create(schema, file);
    GenericRecord previousRecord = null;
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        GenericRecord record = (GenericRecord) (previousRecord == null ? new GenericData.Record(schema) : previousRecord.get("next"));
        record.put("value", (long) i);
        if (previousRecord != null) {
          writer.append(previousRecord);
        }
        GenericRecord nextRecord = new GenericData.Record(record.getSchema());
        record.put("next", nextRecord);
        previousRecord = record;
      }
      writer.append(previousRecord);
    } finally {
      writer.close();
    }

    return file.getName();
  }

  public static AvroTestRecordWriter generateStringAndUtf8Data() throws Exception {

    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
            .namespace("org.apache.drill.exec.store.avro")
            .fields()
            .name("a_string").type().stringBuilder().prop("avro.java.string", "String").endString().noDefault()
            .name("b_utf8").type().stringType().noDefault()
            .endRecord();

    final File file = File.createTempFile("avro-primitive-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {

      ByteBuffer bb = ByteBuffer.allocate(1);
      bb.put(0, (byte) 1);

      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();
        record.put("a_string", "a_" + i);
        record.put("b_utf8", "b_" + i);
        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }

  /**
   * Creates Avro table with specified schema and specified data
   * @param schema table schema
   * @param data table data
   * @param <D> record type
   * @return file with newly created Avro table.
   * @throws IOException if an error is appeared during creation or filling the file.
   */
  public static <D extends SpecificRecord> File write(Schema schema, D... data) throws IOException {
    File file = File.createTempFile("avro-primitive-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());

    DatumWriter writer = SpecificData.get().createDatumWriter(schema);

    try (DataFileWriter<D> fileWriter = new DataFileWriter<>(writer)) {
      fileWriter.create(schema, file);
      for (D datum : data) {
        fileWriter.append(datum);
      }
    }

    return file;
  }

  public static AvroTestRecordWriter generateMapSchema() throws Exception {
    final File file = File.createTempFile("avro-map-test", ".avro", BaseTestQuery.dirTestWatcher.getRootDir());
    final Schema schema = SchemaBuilder.record("AvroRecordReaderTest")
        .namespace("org.apache.drill.exec.store.avro")
        .fields()
        .name("map_field").type().optional().map().values(Schema.create(Type.LONG))
        .name("map_array").type().optional().array().items(Schema.createMap(Schema.create(Type.INT)))
        .name("map_array_value").type().optional().map().values(Schema.createArray(Schema.create(Type.DOUBLE)))
        .endRecord();

    final Schema mapArraySchema = schema.getField("map_array").schema();
    final Schema arrayItemSchema = mapArraySchema.getTypes().get(1);

    final AvroTestRecordWriter record = new AvroTestRecordWriter(schema, file);
    try {
      for (int i = 0; i < RECORD_COUNT; i++) {
        record.startRecord();

        // Create map with long values
        Map<String, Long> map = new HashMap<>();
        map.put("key1", (long) i);
        map.put("key2", (long) i + 1);
        record.put("map_field", map);

        // Create list of map with int values
        GenericArray<Map<String, Integer>> array = new GenericData.Array<>(ARRAY_SIZE, arrayItemSchema);
        for (int j = 0; j < ARRAY_SIZE; j++) {
          Map<String, Integer> mapInt = new HashMap<>();
          mapInt.put("key1", (i + 1) * (j + 50));
          mapInt.put("key2", (i + 1) * (j + 100));
          array.add(mapInt);
        }
        record.put("map_array", array);

        // create map with array value
        Map<String, GenericArray<Double>> mapArrayValue = new HashMap<>();
        GenericArray<Double> doubleArray = new GenericData.Array<>(ARRAY_SIZE, arrayItemSchema);
        for (int j = 0; j < ARRAY_SIZE; j++) {
          doubleArray.add((double) (i + 1) * j);
        }
        mapArrayValue.put("key1", doubleArray);
        mapArrayValue.put("key2", doubleArray);
        record.put("map_array_value", mapArrayValue);

        record.endRecord();
      }
    } finally {
      record.close();
    }

    return record;
  }
}
