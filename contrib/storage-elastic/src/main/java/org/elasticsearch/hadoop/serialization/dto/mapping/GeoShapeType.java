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
package org.elasticsearch.hadoop.serialization.dto.mapping;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.hadoop.serialization.FieldType;

import static org.elasticsearch.hadoop.serialization.FieldType.*;

public enum GeoShapeType implements GeoField {
    POINT(DOUBLE, 1),
    LINE_STRING(DOUBLE, 2),
    POLYGON(DOUBLE, 3),
    MULTI_POINT(DOUBLE, 2),
    MULTI_LINE_STRING(DOUBLE, 3),
    MULTI_POLYGON(DOUBLE, 4),
    GEOMETRY_COLLECTION(OBJECT, 0),
    ENVELOPE(DOUBLE, 2),
    CIRCLE(DOUBLE, 1);

    private final FieldType format;
    private final int arrayDepth;

    private static final Map<String, GeoShapeType> KNOWN_TYPES = new LinkedHashMap<String, GeoShapeType>();

    static {
        for (GeoShapeType geoShapeType : EnumSet.allOf(GeoShapeType.class)) {
            KNOWN_TYPES.put(geoShapeType.name().toLowerCase(Locale.ENGLISH).replace("_", ""), geoShapeType);
        }
    }

    public static GeoShapeType parse(String name) {
        if (name == null) {
            return null;
        }
        return KNOWN_TYPES.get(name.toLowerCase(Locale.ENGLISH));
    }

    GeoShapeType(FieldType format, int arrayDepth) {
        this.format = format;
        this.arrayDepth = arrayDepth;
    }

    @Override
    public FieldType rawType() {
        return format;
    }
    
    @Override
    public int arrayDepth() {
        return arrayDepth;
    }
}
