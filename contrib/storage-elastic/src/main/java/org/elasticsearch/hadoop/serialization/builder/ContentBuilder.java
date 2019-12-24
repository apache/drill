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
package org.elasticsearch.hadoop.serialization.builder;

import java.io.OutputStream;

import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter.Result;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;

@SuppressWarnings("rawtypes")
public class ContentBuilder {

    private final Generator generator;
    private final ValueWriter writer;


    private ContentBuilder(Generator generator, ValueWriter writer) {
        Assert.notNull(generator);
        this.generator = generator;
        this.writer = writer;
    }

    public static ContentBuilder generate(ValueWriter writer) {
        return new ContentBuilder(new JacksonJsonGenerator(new FastByteArrayOutputStream()), writer);
    }

    public static ContentBuilder generate(OutputStream bos, ValueWriter writer) {
        return new ContentBuilder(new JacksonJsonGenerator(bos), writer);
    }

    @SuppressWarnings("unchecked")
    public ContentBuilder value(Object value) {
        Result result = writer.write(value, generator);
        if (!result.isSuccesful()) {
            String message = null;
            if (value == result.unknownValue) {
                message = String.format("Cannot handle type [%s], instance [%s] using writer [%s]", value.getClass(), value, writer);
            }
            else {
                message = String.format("Cannot handle type [%s] within type [%s], instance [%s] within instance [%s] using writer [%s]",
                        result.unknownValue.getClass(), value.getClass(), result.unknownValue, value, writer);
            }
            throw new EsHadoopSerializationException(message);
        }
        return this;
    }

    public ContentBuilder flush() {
        generator.flush();
        return this;
    }

    public OutputStream content() {
        return (OutputStream) generator.getOutputTarget();
    }

    public void close() {
        generator.close();
    }
}