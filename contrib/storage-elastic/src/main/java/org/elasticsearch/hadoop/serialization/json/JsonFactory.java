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
package org.elasticsearch.hadoop.serialization.json;

import java.io.IOException;
import java.util.Iterator;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.util.ObjectUtils;

public abstract class JsonFactory {

    private static final boolean HAS_OBJECT_READER = ObjectUtils.isClassPresent(
            "org.codehaus.jackson.map.ObjectReader", JsonFactory.class.getClassLoader());

    public static <T> ObjectReader objectReader(ObjectMapper mapper, Class<T> clazz) {
        return (HAS_OBJECT_READER ? JacksonObjectReader.reader(mapper, clazz) : BackportedObjectReader.create(mapper, clazz));
    }

    private static class JacksonObjectReader {
        public static <E> ObjectReader reader(final ObjectMapper mapper, final Class<E> clazz) {
            return new ObjectReader() {
                private final org.codehaus.jackson.map.ObjectReader or = mapper.reader(clazz);

                @Override
                public <T> Iterator<T> readValues(JsonParser parser) throws IOException {
                    return or.readValues(parser);
                }
            };
        }
    }
}
