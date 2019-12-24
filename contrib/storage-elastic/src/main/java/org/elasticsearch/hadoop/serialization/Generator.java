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


public interface Generator extends Closeable {

    void writeBeginArray();

    void writeEndArray();

    void writeBeginObject();

    void writeEndObject();

    void writeFieldName(String name);

    void writeString(String text);

    void writeUTF8String(byte[] text, int offset, int len);

    void writeUTF8String(byte[] text);

    void writeBinary(byte[] data, int offset, int len);

    void writeBinary(byte[] data);

    void writeNumber(short s);

    void writeNumber(byte b);

    void writeNumber(int i);

    void writeNumber(long l);

    void writeNumber(double d);

    void writeNumber(float f);

    void writeBoolean(boolean b);

    void writeNull();

    void flush();

    void close();

    Object getOutputTarget();

    String getParentPath();
}
