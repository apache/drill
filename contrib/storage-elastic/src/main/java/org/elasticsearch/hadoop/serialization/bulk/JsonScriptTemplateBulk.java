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
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.field.JsonFieldExtractors;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;

class JsonScriptTemplateBulk extends JsonTemplatedBulk {

    private static final Log log = LogFactory.getLog(JsonTemplatedBulk.class);
    private final BytesArray scratchPad = new BytesArray(1024);

    public JsonScriptTemplateBulk(Collection<Object> beforeObject, Collection<Object> afterObject,
            JsonFieldExtractors jsonExtractors, Settings settings) {
        super(beforeObject, afterObject, jsonExtractors, settings);
    }

    @Override
    protected Object preProcess(Object object, BytesArray storage) {
        // serialize the json early on and copy it to storage
        Assert.notNull(object, "Empty/null JSON document given...");

        BytesArray ba = null;
        if (ConfigurationOptions.ES_OPERATION_UPSERT.equals(settings.getOperation())) {
            ba = storage;
        }
        else {
            scratchPad.reset();
            ba = scratchPad;
        }

        // write the doc to a temporary space
        jsonWriter.convert(object, ba);

        if (log.isTraceEnabled()) {
            log.trace(String.format("About to extract information from [%s]", ba));
        }

        jsonExtractors.process(ba);
        return storage;
    }
}
