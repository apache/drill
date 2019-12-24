/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.serialization;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

public enum FieldType {
    // core Types
    NULL,
    BOOLEAN,
    BYTE,
    SHORT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    DATE,
    BINARY,
    TOKEN_COUNT,

    GEO_POINT,
    GEO_SHAPE,

    // compound types
    OBJECT,
    NESTED,

    // not supported yet
    IP,

    // ignored
    COMPLETION;

    private static final Set<String> KNOWN_TYPES = new LinkedHashSet<String>();

    static {
        for (FieldType fieldType : EnumSet.allOf(FieldType.class)) {
            KNOWN_TYPES.add(fieldType.name());
        }
    }

    public static FieldType parse(String name) {
        if (name == null) {
            return null;
        }
        String n = name.toUpperCase(Locale.ENGLISH);
        return (KNOWN_TYPES.contains(n) ? FieldType.valueOf(n) : null);
    }

    public static boolean isRelevant(FieldType fieldType) {
        if (fieldType == null || COMPLETION == fieldType) {
            return false;
        }

        return true;
    }

    public static boolean isCompound(FieldType fieldType) {
        return (OBJECT == fieldType || NESTED == fieldType);
    }

    public static boolean isGeo(FieldType fieldType) {
        return (GEO_POINT == fieldType || GEO_SHAPE == fieldType);
    }
}