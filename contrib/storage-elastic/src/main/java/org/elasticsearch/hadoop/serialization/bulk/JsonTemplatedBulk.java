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
package org.elasticsearch.hadoop.serialization.bulk;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.BytesConverter;
import org.elasticsearch.hadoop.serialization.builder.NoOpValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;
import org.elasticsearch.hadoop.serialization.field.JsonFieldExtractors;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.ObjectUtils;

/**
 * Dedicated JSON command that skips the content generation phase (since the data is already JSON).
 */
class JsonTemplatedBulk extends TemplatedBulk {

    private static Log log = LogFactory.getLog(JsonTemplatedBulk.class);

    protected final JsonFieldExtractors jsonExtractors;
    protected final BytesConverter jsonWriter;
    protected final Settings settings;

    public JsonTemplatedBulk(Collection<Object> beforeObject, Collection<Object> afterObject,
            JsonFieldExtractors jsonExtractors, Settings settings) {
        super(beforeObject, afterObject, new NoOpValueWriter());
        this.jsonExtractors = jsonExtractors;
        this.jsonWriter = ObjectUtils.instantiate(settings.getSerializerBytesConverterClassName(), settings);
        this.settings = settings;
    }

    @Override
    protected Object preProcess(Object object, BytesArray storage) {
        // serialize the json early on and copy it to storage
        Assert.notNull(object, "Empty/null JSON document given...");

        jsonWriter.convert(object, storage);

        if (log.isTraceEnabled()) {
            log.trace(String.format("About to extract information from [%s]", storage));
        }

        jsonExtractors.process(storage);
        return storage;
    }

    @Override
    protected void doWriteObject(Object object, BytesArray storage, ValueWriter<?> writer) {
        // no-op - the object has been already serialized to storage
    }
}
