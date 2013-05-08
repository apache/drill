/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.schema.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

import org.apache.drill.exec.schema.*;

import java.io.*;
import java.util.Map;

public class ScanJson extends PhysicalOperator {
    private ScanJsonIterator iterator;

    private static final Map<JsonToken, ReadType> READ_TYPES = Maps.newHashMap();

    static {
        READ_TYPES.put(JsonToken.START_ARRAY, ReadType.ARRAY);
        READ_TYPES.put(JsonToken.START_OBJECT, ReadType.OBJECT);
    }

    public ScanJson(String inputName) throws IOException {
        super();
        this.iterator = new ScanJsonIterator(inputName);
    }

    @Override
    public PhysicalOperatorIterator getIterator() {
        return iterator;
    }

    class ScanJsonIterator implements PhysicalOperatorIterator {
        private JsonParser parser;
        private SchemaRecorder recorder;
        private BackedRecord record;
        private IdGenerator generator;

        private ScanJsonIterator(String inputName) throws IOException {
            InputSupplier<InputStreamReader> input;
            if (inputName.startsWith("resource:")) {
                input = Resources.newReaderSupplier(Resources.getResource(inputName.substring(inputName.indexOf(':') + 1)), Charsets.UTF_8);
            } else {
                input = Files.newReaderSupplier(new File(inputName), Charsets.UTF_8);
            }

            JsonFactory factory = new JsonFactory();
            parser = factory.createJsonParser(input.getInput());
            parser.nextToken(); // Read to the first START_OBJECT token
            recorder = new SchemaRecorder();
            generator = new SchemaIdGenerator();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public NextOutcome next() throws IOException {
            if (parser.isClosed() || !parser.hasCurrentToken()) {
                return NextOutcome.NONE_LEFT;
            }

            recorder.reset();

            DataRecord dataRecord = new DataRecord();
            ReadType.OBJECT.readRecord(parser, generator, recorder, dataRecord, null);

            parser.nextToken(); // Read to START_OBJECT token

            if (!parser.hasCurrentToken()) {
                parser.close();
            }

            recorder.addMissingFields();
            if (record == null) {
                record = new BackedRecord(recorder.getDiffSchema(), dataRecord);
            } else {
                record.setBackend(recorder.getDiffSchema(), dataRecord);
            }
            return recorder.hasDiffs() ? NextOutcome.INCREMENTED_SCHEMA_CHANGED : NextOutcome.INCREMENTED_SCHEMA_UNCHANGED;
        }

        public RecordSchema getCurrentSchema() {
            return recorder.getCurrentSchema();
        }
    }

    public static enum ReadType {
        ARRAY(JsonToken.END_ARRAY) {
            @Override
            public Field createField(RecordSchema parentSchema, IdGenerator<Integer> generator, String prefixFieldName, String fieldName, Field.FieldType fieldType, int index) {
                return new OrderedField(parentSchema, generator, fieldType, prefixFieldName, index);
            }

            @Override
            public RecordSchema createSchema() throws IOException {
                return new ListSchema();
            }
        },
        OBJECT(JsonToken.END_OBJECT) {
            @Override
            public Field createField(RecordSchema parentSchema, IdGenerator<Integer> generator, String prefixFieldName, String fieldName, Field.FieldType fieldType, int index) {
                return new NamedField(parentSchema, generator, prefixFieldName, fieldName, fieldType);
            }

            @Override
            public RecordSchema createSchema() throws IOException {
                return new ObjectSchema();
            }
        };

        private final JsonToken endObject;

        ReadType(JsonToken endObject) {
            this.endObject = endObject;
        }

        public JsonToken getEndObject() {
            return endObject;
        }

        public void readRecord(JsonParser parser, IdGenerator generator, SchemaRecorder recorder, DataRecord record, String prefixFieldName) throws IOException {
            JsonToken token = parser.nextToken();
            JsonToken endObject = getEndObject();
            int index = 0;
            while (token != endObject) {
                if (token == JsonToken.FIELD_NAME) {
                    token = parser.nextToken();
                    continue;
                }

                String fieldName = parser.getCurrentName();
                Field.FieldType fieldType = JacksonHelper.getFieldType(token);
                ReadType readType = READ_TYPES.get(token);
                if (fieldType != null) { // Including nulls
                    recorder.recordData(this, readType, parser, generator, record, fieldType, prefixFieldName, fieldName, index);
                }
                token = parser.nextToken();
                ++index;
            }
        }

        public abstract RecordSchema createSchema() throws IOException;

        public abstract Field createField(RecordSchema parentSchema, IdGenerator<Integer> generator, String prefixFieldName, String fieldName, Field.FieldType fieldType, int index);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Requires json path: ScanJson <json_path>");
            return;
        }

        String jsonPath = args[0];

        System.out.println("Reading json input...");
        ScanJson sj = new ScanJson(jsonPath);
        ScanJsonIterator iterator = (ScanJsonIterator) sj.getIterator();
        long count = 0;

        while (iterator.next() != PhysicalOperatorIterator.NextOutcome.NONE_LEFT) {
            Record record = iterator.getRecord();
            System.out.println("Record " + ++count);
            System.out.println("Schema: ");
            System.out.println(iterator.getCurrentSchema().toSchemaString());
            System.out.println();
            System.out.println("Changes since last record: ");
            System.out.println();
            System.out.println(record.getSchemaChanges());
            System.out.println();
        }
    }
}






