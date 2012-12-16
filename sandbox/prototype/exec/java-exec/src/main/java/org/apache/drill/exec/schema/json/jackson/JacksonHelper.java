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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.Maps;

import com.sun.javaws.exceptions.InvalidArgumentException;
import org.apache.drill.exec.schema.Field;

import java.io.IOException;
import java.util.EnumMap;

import static com.fasterxml.jackson.core.JsonToken.*;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.drill.exec.schema.Field.FieldType.*;

public class JacksonHelper {
    public static Field.FieldType getFieldType(JsonToken token) {
        switch(token) {
            case VALUE_STRING:
                return STRING;
            case VALUE_FALSE:
                return BOOLEAN;
            case VALUE_TRUE:
                return BOOLEAN;
            case START_ARRAY:
                return ARRAY;
            case START_OBJECT:
                return MAP;
            case VALUE_NUMBER_INT:
                return INTEGER;
            case VALUE_NUMBER_FLOAT:
                return FLOAT;
        }

        throw new UnsupportedOperationException();
    }

    public static Object getValueFromFieldType(JsonParser parser, Field.FieldType fieldType) throws IOException {
        switch (fieldType) {
            case INTEGER:
                return parser.getIntValue();
            case STRING:
                return parser.getValueAsString();
            case FLOAT:
                return parser.getFloatValue();
            case BOOLEAN:
                return parser.getBooleanValue();
            default:
                throw new RuntimeException("Unexpected Field type to return value: " + fieldType.toString());
        }
    }
}
