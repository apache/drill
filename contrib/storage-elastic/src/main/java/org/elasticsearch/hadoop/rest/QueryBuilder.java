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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * 在这里进行拼sql查询了
 */
public class QueryBuilder {
    private static final Log log = LogFactory.getLog(QueryBuilder.class);
    private final Resource resource;

    private final Map<String, String> uriParams = new LinkedHashMap<String, String>();
    private BytesArray bodyQuery;

    private TimeValue time = TimeValue.timeValueMinutes(10);
    private long size = 50;
    private long limit = -1;
    private String shard;
    private String node;
    private boolean onlyNode;
    private String[] filters;
    private int maxBigFieldLength = 3000; //http请求串最大字符数
    private final boolean IS_ES_20;
    private final boolean INCLUDE_VERSION;

    private String fields;

    QueryBuilder(Settings settings) {
        this.resource = new Resource(settings, true);
        IS_ES_20 = SettingsUtils.isEs20(settings);
        INCLUDE_VERSION = settings.getReadMetadata() && settings.getReadMetadataVersion();

        if (StringUtils.hasText(settings.getProperty(ConfigurationOptions.ES_SCROLL_ESCAPE_QUERY_URI))) {
            LogFactory.getLog(ConfigurationOptions.class).warn(String
                    .format("Setting '%s' has been deprecated as the URI queries are _always_ translated into a Query DSL; see the documentation for more information",
                            ConfigurationOptions.ES_SCROLL_ESCAPE_QUERY_URI));
        }

        bodyQuery = QueryUtils.parseQuery(settings);
    }

    public static QueryBuilder query(Settings settings) {
        return new QueryBuilder(settings).
                time(settings.getScrollKeepAlive()).
                size(settings.getScrollSize()).
                limit(settings.getScrollLimit());
    }

    public QueryBuilder size(long size) {
        this.size = size;
        return this;
    }

    public QueryBuilder limit(long limit) {
        this.limit = limit;
        return this;
    }

    public QueryBuilder time(long timeInMillis) {
        Assert.isTrue(timeInMillis > 0, "Invalid time");
        this.time = TimeValue.timeValueMillis(timeInMillis);
        return this;
    }

    public QueryBuilder node(String node) {
        Assert.hasText(node, "Invalid node");
        this.node = node;
        return this;
    }

    public QueryBuilder shard(String shard) {
        Assert.hasText(shard, "Invalid shard");
        this.shard = shard;
        return this;
    }

    public QueryBuilder fields(String fieldsCSV) {
        this.fields = fieldsCSV;
        return this;
    }

    public QueryBuilder filter(String... filters) {
        this.filters = filters;
        return this;
    }

    private String assemble() {
        if (limit > 0) {
            if (size > limit) {
                size = limit;
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append(StringUtils.encodePath(resource.index()));
        sb.append("/");
        sb.append(StringUtils.encodePath(resource.type()));
        sb.append("/_search?");

        // override infrastructure params
        uriParams.put("search_type", "scan");
        uriParams.put("scroll", String.valueOf(time.toString()));
        uriParams.put("size", String.valueOf(size));
        if (INCLUDE_VERSION) {
            uriParams.put("version", "");
        }

        // override fields
        if (StringUtils.hasText(fields)) {
            // ES 1.0 当这些字段太长时，发起http请求会有问题
            if(fields.length() < maxBigFieldLength){
                uriParams.put("_source", fields);
            } else {
                uriParams.put("_source", "true");
            }
            uriParams.remove("fields");
        }
        else {
            uriParams.remove("fields");
        }

        StringBuilder pref = new StringBuilder();
        if (StringUtils.hasText(shard)) {
            pref.append("_shards:");
            pref.append(shard);
        }
        if (StringUtils.hasText(node)) {
            if (pref.length() > 0) {
                pref.append(";");
            }
            pref.append(onlyNode ? "_only_node:" : "_prefer_node:");
            pref.append(node);
        }

        if (pref.length() > 0) {
            uriParams.put("preference", pref.toString());
        }

        // append params
        for (Iterator<Entry<String, String>> it = uriParams.entrySet().iterator(); it.hasNext();) {
            Entry<String, String> entry = it.next();
            sb.append(entry.getKey());
            if (StringUtils.hasText(entry.getValue())) {
                sb.append("=");
                sb.append(entry.getValue());
            }
            if (it.hasNext()) {
                sb.append("&");
            }
        }

        return sb.toString();
    }

    public ScrollQuery build(RestRepository client, ScrollReader reader) {
        String scrollUri = assemble();
//        if (StringUtils.hasText(fields) && fields.length() >= maxBigFieldLength) {
//            // ES 1.0 当这些字段太长时，发起http请求会有问题
//            bodyQuery = QueryUtils.applyFiltersTooBigSize(fields ,bodyQuery, filters   );
//        } else {
//            bodyQuery = QueryUtils.applyFilters(bodyQuery, filters);
//        }

        bodyQuery = QueryUtils.applyFilters(bodyQuery, filters);
        return client.scanLimit(scrollUri, bodyQuery, limit, reader);
    }

    @Override
    public String toString() {
        return "QueryBuilder [" + assemble() + "]";
    }

    public QueryBuilder restrictToNode(boolean onlyNode) {
        this.onlyNode = onlyNode;
        return this;
    }
}