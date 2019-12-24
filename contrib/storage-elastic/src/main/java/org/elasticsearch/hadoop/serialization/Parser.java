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

import java.io.Closeable;

public interface Parser extends Closeable {
    enum Token {
        START_OBJECT {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        END_OBJECT {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        START_ARRAY {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        END_ARRAY {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        FIELD_NAME {
            @Override
            public boolean isValue() {
                return false;
            }
        },

        VALUE_STRING {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        VALUE_NUMBER {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        VALUE_BOOLEAN {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        // usually a binary value
        VALUE_EMBEDDED_OBJECT {
            @Override
            public boolean isValue() {
                return true;
            }
        },

        VALUE_NULL {
            @Override
            public boolean isValue() {
                return true;
            }
        };

        public abstract boolean isValue();
    }


    enum NumberType {
        INT, LONG, FLOAT, DOUBLE, BIG_INTEGER, BIG_DECIMAL
    }

    Token currentToken();

    Token nextToken();

    void skipChildren();

    String absoluteName();

    String currentName();

    Object currentValue();

    String text();

    byte[] bytes();

    Number numberValue();

    NumberType numberType();

    short shortValue();

    int intValue();

    long longValue();

    float floatValue();

    double doubleValue();

    boolean booleanValue();

    byte[] binaryValue();

    @Override
    void close();

    // Fairly experimental methods

    // method _highly_ dependent on the underlying implementation and given source
    // returns the current position inside the parsed stream
    // the returned value is highly dependent on the implementation used - currently Jackson
    // this is typically used when reading known content such as Elasticsearch response
    int tokenCharOffset();
}