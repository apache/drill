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
package org.elasticsearch.hadoop.rest;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;


/**
 * ElasticSearch Rest Resource - index and type.
 * 由于在实时查询时，默认都会加上数据库前缀的，所以要在这里进行解决
 */
public class Resource {

    private final String indexAndType;
    private final String type;
    private final String index;
    private final String bulk;
    private final String refresh;

    public Resource(Settings settings, boolean read) {
        String resource = (read ? settings.getResourceRead() : settings.getResourceWrite());
        String rtqueryDatabases = settings.getRtqueryDatabases();

        if(resource.indexOf(".") == -1 && (rtqueryDatabases != null && !"".equals(rtqueryDatabases)) ){
            //当要查询的表没有db数据库前缀时，会在es中找不到template，会出错，所以相关的读取资源索引要在前面加上db前缀
            resource = rtqueryDatabases + "." + resource;
        }

        String errorMessage = "invalid resource given; expecting [index]/[type] - received ";
        Assert.hasText(resource, errorMessage + resource);

        // add compatibility for now
        if (resource.contains("?") || resource.contains("&")) {
            if (!StringUtils.hasText(settings.getQuery())) {
                throw new EsHadoopIllegalArgumentException(String.format(
                        "Cannot specify a query in the target index and through %s", ConfigurationOptions.ES_QUERY));
            }

            // extract query
            int index = resource.indexOf("?");
            if (index > 0) {
                String query = resource.substring(index);

                // clean resource
                resource = resource.substring(0, index);
                index = resource.lastIndexOf("/");
                resource = (index > 0 ? resource.substring(0, index) : resource);

                settings.setProperty(ConfigurationOptions.ES_RESOURCE, resource);
                settings.setQuery(query);
            }
        }

        String res = StringUtils.sanitizeResource(resource);

        int slash = res.indexOf("/");
        if (slash < 0) {
            index = res;
            type = StringUtils.EMPTY;
        }
        else {
            index = res.substring(0, slash);
            type = res.substring(slash + 1);

            Assert.hasText(type, "No type found; expecting [index]/[type]");
        }
        Assert.hasText(index, "No index found; expecting [index]/[type]");

        indexAndType = index + "/" + type;

        // check bulk
        bulk = (indexAndType.contains("{") ? "/_bulk" : indexAndType + "/_bulk");
        refresh = (index.contains("{") ? "/_refresh" : index + "/_refresh");
    }

    String bulk() {
        return bulk;
    }

    String mapping() {
        return indexAndType + "/_mapping";
    }

    String aliases() {
        return index + "/_aliases";
    }

    String indexAndType() {
        return indexAndType;
    }

    String type() {
        return type;
    }

    String index() {
        return index;
    }

    @Override
    public String toString() {
        return indexAndType;
    }

    public String refresh() {
        return refresh;
    }
}