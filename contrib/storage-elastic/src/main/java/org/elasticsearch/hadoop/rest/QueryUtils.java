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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.StringUtils;

abstract class QueryUtils {

    // translate URI query into Query DSL

    private static String QUERY_STRING_QUERY = "{\"query\":{\"query_string\":{ %s }}}";
    static String MATCH_ALL = "{\"query\":{\"match_all\":{}}}";

    private static String QUOTE = "\"";
    private static Map<String, String> URI_QUERY_TO_DSL = new HashMap<String, String>();

    // query used for push down functionality
    // the query part is expected in full form: `"query" : { ... } `
    // while the filter part with the leading { and trailing }: `{ "prefix" : {} }, {"range" : {} }, ... }`

    private static String PUSH_DOWN = "{\"query\":{" +
            "\"filtered\":{ " +
            // put query first,
            "%s" +
            "," +
            // followed by the filters ("and" being applied all the time)
            "\"filter\": { \"and\" : [ %s ] } " +
            "}}}";

    private static String FIELDS_PUSH_DOWN = "{\"fields\":[%s], \"query\":{" +
            "\"filtered\":{ " +
            // put query first,
            "%s" +
            "," +
            // followed by the filters ("and" being applied all the time)
            "\"filter\": { \"and\" : [ %s ] } " +
            "}}}";

    private static String FIELDS_EMPTY_FILE = "{\"fields\":[%s] }";


    static {
        URI_QUERY_TO_DSL.put("q", "query");
        URI_QUERY_TO_DSL.put("df", "default_field");
        URI_QUERY_TO_DSL.put("analyzer", "analyzer");
        URI_QUERY_TO_DSL.put("lowercase_expanded_terms", "lowercase_expanded_terms");
        URI_QUERY_TO_DSL.put("analyze_wildcard", "analyze_wildcard");
        URI_QUERY_TO_DSL.put("default_operator", "default_operator");
        URI_QUERY_TO_DSL.put("lenient", "lenient");
    }


    static BytesArray parseQuery(Settings settings) {
        String query = settings.getQuery();
        if (!StringUtils.hasText(query)) {
            query = MATCH_ALL;
        }

        query = query.trim();

        // uri query
        if (query.startsWith("?")) {
            return new BytesArray(QueryUtils.translateUriQuery(query, settings.getScrollEscapeUri()));
        }
        else if (query.startsWith("{")) {
            return new BytesArray(query);
        }
        else {
            try {
                // must be a resource
                InputStream in = settings.loadResource(query);
                // peek the stream
                int first = in.read();
                if (Integer.valueOf('?').equals(first)) {
                    return new BytesArray(QueryUtils.translateUriQuery(IOUtils.asString(in), settings.getScrollEscapeUri()));
                }
                else {
                    BytesArray content = new BytesArray(1024);
                    content.add(first);
                    IOUtils.asBytes(content, in);
                    return content;
                }
            } catch (IOException ex) {
                throw new EsHadoopIllegalArgumentException(
                        String.format(
                                "Cannot determine specified query - doesn't appear to be URI or JSON based and location [%s] cannot be opened",
                                query));
            }
        }
    }

    private static String translateUriQuery(String query, boolean escapeUriQuery) {
        // strip leading ?
        if (query.startsWith("?")) {
            query = query.substring(1);
        }

        // break down the uri into parameters
        Map<String, String> params = new LinkedHashMap<String, String>();
        for (String token : query.split("&")) {
            int indexOf = token.indexOf("=");
            Assert.isTrue(indexOf > 0, String.format("Cannot token [%s] in uri query [%s]", token, query));
            if (!escapeUriQuery) {
                params.put(StringUtils.decodePath(token.substring(0, indexOf)),
                        StringUtils.decodePath(token.substring(indexOf + 1)));
            }
            else {
                params.put(token.substring(0, indexOf), token.substring(indexOf + 1));
            }
        }

        Map<String, String> translated = new LinkedHashMap<String, String>();

        for (Entry<String, String> entry : params.entrySet()) {
            String translatedKey = URI_QUERY_TO_DSL.get(entry.getKey());
            Assert.hasText(translatedKey, String.format("Unknown '%s' parameter; please change the URI query into a Query DLS (see 'Query String Query')", entry.getKey()));
            translated.put(translatedKey, entry.getValue());
        }

        // check whether a query is specified
        if (translated.containsKey("query")) {
            StringBuilder sb = new StringBuilder();

            for (Entry<String, String> entry : translated.entrySet()) {
                sb.append(addQuotes(entry.getKey()));
                sb.append(":");
                sb.append(addQuotes(entry.getValue()));
                sb.append(",");
            }

            // translate the Uri params as a Query String Query
            return String.format(QUERY_STRING_QUERY, sb.substring(0, sb.length() - 1));

        }
        return MATCH_ALL;
    }


    private static String addQuotes(String value) {
        boolean lead = value.startsWith(QUOTE);
        boolean trail = value.endsWith(QUOTE);

        if (lead && trail) {
            return value;
        }

        StringBuilder sb = new StringBuilder();
        if (!lead) {
            sb.append(QUOTE);
        }
        sb.append(value);
        if (!trail) {
            sb.append(QUOTE);
        }

        return sb.toString();
    }

    /**
     * 当请求的field太长时，就会有问题
     * @param fields
     * @param bodyQuery
     * @param filters
     * @return
     */
    static BytesArray applyFiltersTooBigSize( String fields ,BytesArray bodyQuery, String... filters) {
        if (filters == null || filters.length == 0) {
            return new BytesArray(String.format(FIELDS_EMPTY_FILE, "\"" + StringUtils.concatenate(fields.split(","), "\",\"") + "\"" ));
        }

        String originalQuery = bodyQuery.toString();
        // remove leading/trailing { }
        int start = originalQuery.indexOf("{");
        int stop = originalQuery.lastIndexOf("}");

        String msg = String.format("Cannot apply filter(s) to what looks like an invalid DSL query (no leading/trailing { } ): '%s' ", originalQuery);
        Assert.isTrue(start >= 0, msg);
        Assert.isTrue(stop >= 0, msg);
        Assert.isTrue(stop - start > 0, msg);

        String nestedQuery = originalQuery.substring(start + 1, stop);

        // concatenate filters
        return new BytesArray(String.format(FIELDS_PUSH_DOWN, "\"" + StringUtils.concatenate(fields.split(","), "\",\"") + "\""
                , nestedQuery, StringUtils.concatenate(filters, ",")));
    }

    static BytesArray applyFilters(BytesArray bodyQuery, String... filters) {
        if (filters == null || filters.length == 0) {
            return bodyQuery;
        }

        String originalQuery = bodyQuery.toString();
        // remove leading/trailing { }
        int start = originalQuery.indexOf("{");
        int stop = originalQuery.lastIndexOf("}");

        String msg = String.format("Cannot apply filter(s) to what looks like an invalid DSL query (no leading/trailing { } ): '%s' ", originalQuery);
        Assert.isTrue(start >= 0, msg);
        Assert.isTrue(stop >= 0, msg);
        Assert.isTrue(stop - start > 0, msg);

        String nestedQuery = originalQuery.substring(start + 1, stop);

        // concatenate filters
        return new BytesArray(String.format(PUSH_DOWN, nestedQuery, StringUtils.concatenate(filters, ",")));
    }
}