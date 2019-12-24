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

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;

/**
 * Handles the instantiation of bulk commands.
 */
public abstract class BulkCommands {

    public static BulkCommand create(Settings settings, MetadataExtractor metaExtractor) {

        String operation = settings.getOperation();
        BulkFactory factory = null;

        if (ConfigurationOptions.ES_OPERATION_CREATE.equals(operation)) {
            factory = new CreateBulkFactory(settings, metaExtractor);
        }
        else if (ConfigurationOptions.ES_OPERATION_INDEX.equals(operation)) {
            factory = new IndexBulkFactory(settings, metaExtractor);
        }
        else if (ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation)) {
            factory = new UpdateBulkFactory(settings, metaExtractor);
        }
        else if (ConfigurationOptions.ES_OPERATION_UPSERT.equals(operation)) {
            factory = new UpdateBulkFactory(settings, true, metaExtractor);
        }
        else {
            throw new EsHadoopIllegalArgumentException("Unknown operation " + operation);
        }

        return factory.createBulk();
    }
}
