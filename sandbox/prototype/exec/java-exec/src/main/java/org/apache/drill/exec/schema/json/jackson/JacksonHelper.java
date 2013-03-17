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

import org.apache.drill.exec.schema.Field;

import java.io.IOException;
import java.util.EnumMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class JacksonHelper {
    private static final EnumMap<JsonToken, Field.FieldType> TYPE_LOOKUP = Maps.newEnumMap(JsonToken.class);

    static {
        TYPE_LOOKUP.put(JsonToken.VALUE_STRING, Field.FieldType.STRING);
        TYPE_LOOKUP.put(JsonToken.VALUE_FALSE, Field.FieldType.BOOLEAN);
        TYPE_LOOKUP.put(JsonToken.VALUE_TRUE, Field.FieldType.BOOLEAN);
        TYPE_LOOKUP.put(JsonToken.START_ARRAY, Field.FieldType.ARRAY);
        TYPE_LOOKUP.put(JsonToken.START_OBJECT, Field.FieldType.MAP);
        TYPE_LOOKUP.put(JsonToken.VALUE_NUMBER_INT, Field.FieldType.INTEGER);
        TYPE_LOOKUP.put(JsonToken.VALUE_NUMBER_FLOAT, Field.FieldType.FLOAT);
    }

    public static Field.FieldType getFieldType(JsonToken token) {
        return TYPE_LOOKUP.get(token);
    }

    public static Object getValueFromFieldType(JsonParser parser, Field.FieldType fieldType) throws IOException {
        switch(fieldType) {
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
