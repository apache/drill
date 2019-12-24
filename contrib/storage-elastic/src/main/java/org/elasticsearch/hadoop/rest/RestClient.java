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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.Request.Method;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.ParsingUtils;
import org.elasticsearch.hadoop.serialization.dto.Node;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.serialization.json.JsonFactory;
import org.elasticsearch.hadoop.serialization.json.ObjectReader;
import org.elasticsearch.hadoop.util.ByteSequence;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.elasticsearch.hadoop.util.unit.TimeValue;

import static org.elasticsearch.hadoop.rest.Request.Method.*;

public class RestClient implements Closeable, StatsAware {

    private NetworkClient network;
    private final ObjectMapper mapper;
    private final TimeValue scrollKeepAlive;
    private final boolean indexReadMissingAsEmpty;
    private final HttpRetryPolicy retryPolicy;

    private final boolean isES20;

    {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.USE_ANNOTATIONS, false);
        mapper.configure(SerializationConfig.Feature.USE_ANNOTATIONS, false);
    }


    private final Stats stats = new Stats();

    public enum HEALTH {
        RED, YELLOW, GREEN
    }

    public RestClient(Settings settings) {
        network = new NetworkClient(settings);

        scrollKeepAlive = TimeValue.timeValueMillis(settings.getScrollKeepAlive());
        indexReadMissingAsEmpty = settings.getIndexReadMissingAsEmpty();

        String retryPolicyName = settings.getBatchWriteRetryPolicy();

        if (ConfigurationOptions.ES_BATCH_WRITE_RETRY_POLICY_SIMPLE.equals(retryPolicyName)) {
            retryPolicyName = SimpleHttpRetryPolicy.class.getName();
        }
        else if (ConfigurationOptions.ES_BATCH_WRITE_RETRY_POLICY_NONE.equals(retryPolicyName)) {
            retryPolicyName = NoHttpRetryPolicy.class.getName();
        }

        retryPolicy = ObjectUtils.instantiate(retryPolicyName, settings);

        isES20 = SettingsUtils.isEs20(settings);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List<String> discoverNodes() {
        String endpoint = "_nodes/transport";
        Map<String, Map> nodes = (Map<String, Map>) get(endpoint, "nodes");

        List<String> hosts = new ArrayList<String>(nodes.size());

        for (Map value : nodes.values()) {
            String inet = (String) value.get("http_address");
            if (StringUtils.hasText(inet)) {
                hosts.add(StringUtils.parseIpAddress(inet).toString());
            }
        }

        return hosts;
    }

    private <T> T get(String q, String string) {
        return parseContent(execute(GET, q), string);
    }

    @SuppressWarnings("unchecked")
    private <T> T parseContent(InputStream content, String string) {
        Map<String, Object> map = Collections.emptyMap();

        try {
            // create parser manually to lower Jackson requirements
            JsonParser jsonParser = mapper.getJsonFactory().createJsonParser(content);
            try {
                map = mapper.readValue(jsonParser, Map.class);
            } finally {
                countStreamStats(content);
            }
        } catch (IOException ex) {
            throw new EsHadoopParsingException(ex);
        }

        return (T) (string != null ? map.get(string) : map);
    }

    public BitSet bulk(Resource resource, TrackingBytesArray data) {
        Retry retry = retryPolicy.init();
        int httpStatus = 0;

        boolean isRetry = false;

        do {
            // NB: dynamically get the stats since the transport can change
            long start = network.transportStats().netTotalTime;
            Response response = execute(PUT, resource.bulk(), data);
            long spent = network.transportStats().netTotalTime - start;

            stats.bulkTotal++;
            stats.docsSent += data.entries();
            stats.bulkTotalTime += spent;
            // bytes will be counted by the transport layer

            if (isRetry) {
                stats.docsRetried += data.entries();
                stats.bytesRetried += data.length();
                stats.bulkRetries++;
                stats.bulkRetriesTotalTime += spent;
            }

            isRetry = true;

            httpStatus = (retryFailedEntries(response, data) ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.OK);
        } while (data.length() > 0 && retry.retry(httpStatus));

        return data.leftoversPosition();
    }

    @SuppressWarnings("rawtypes")
    boolean retryFailedEntries(Response response, TrackingBytesArray data) {
        InputStream content = response.body();
        try {
            ObjectReader r = JsonFactory.objectReader(mapper, Map.class);
            JsonParser parser = mapper.getJsonFactory().createJsonParser(content);
            try {
                if (ParsingUtils.seek(new JacksonJsonParser(parser), "items") == null) {
                    // recorded bytes are ack here
                    stats.bytesAccepted += data.length();
                    stats.docsAccepted += data.entries();
                    return false;
                }
            } finally {
                countStreamStats(content);
            }

            int entryToDeletePosition = 0; // head of the list
            for (Iterator<Map> iterator = r.readValues(parser); iterator.hasNext();) {
                Map map = iterator.next();
                Map values = (Map) map.values().iterator().next();
                Integer status = (Integer) values.get("status");

                String error = extractError(values);
                if (error != null) {
                    if ((status != null && HttpStatus.canRetry(status)) || error.contains("EsRejectedExecutionException")) {
                        entryToDeletePosition++;
                    }
                    else {
                        String message =  (status != null ?
                                String.format("[%s] returned %s(%s) - %s", response.uri(), HttpStatus.getText(status), status, prettify(error)) : prettify(error));
                        throw new EsHadoopInvalidRequest(String.format("Found unrecoverable error %s; Bailing out..", message));
                    }
                }
                else {
                    stats.bytesAccepted += data.length(entryToDeletePosition);
                    stats.docsAccepted += 1;
                    data.remove(entryToDeletePosition);
                }
            }

            return entryToDeletePosition > 0;
            // catch IO/parsing exceptions
        } catch (IOException ex) {
            throw new EsHadoopParsingException(ex);
        }
    }

    private String extractError(Map jsonMap) {
        Object err = jsonMap.get("error");
        String error = null;
        if (err != null) {
            // part of ES 2.0
            if (err instanceof Map) {
                Map m = ((Map) err);
                err = m.get("root_cause");
                if (err == null) {
                    error = m.get("reason").toString();
                    if (m.containsKey("caused_by")) {
                        error += ";" + ((Map) m.get("caused_by")).get("reason");
                    }
                }
                else {
                    if (err instanceof List) {
                        Object nested = ((List) err).get(0);
                        if (nested instanceof Map) {
                            Map nestedM = (Map) nested;
                            if (nestedM.containsKey("reason")) {
                                error = nestedM.get("reason").toString();
                            }
                            else {
                                error = nested.toString();
                            }
                        }
                        else {
                            error = nested.toString();
                        }
                    }
                    else {
                        error = err.toString();
                    }
                }
            }
            else {
                error = err.toString();
            }
        }
        return error;
    }

    private String prettify(String error) {
        if (isES20) {
            return error;
        }

        String invalidFragment = ErrorUtils.extractInvalidXContent(error);
        String header = (invalidFragment != null ? "Invalid JSON fragment received[" + invalidFragment + "]" : "");
        return header + "[" + error + "]";
    }

    private String prettify(String error, ByteSequence body) {
        if (isES20) {
            return error;
        }
        String message = ErrorUtils.extractJsonParse(error, body);
        return (message != null ? error + "; fragment[" + message + "]" : error);
    }

    public void refresh(Resource resource) {
        execute(POST, resource.refresh());
    }

    public List<List<Map<String, Object>>> targetShards(String index) {
        List<List<Map<String, Object>>> shardsJson = null;

        // https://github.com/elasticsearch/elasticsearch/issues/2726
        String target = index + "/_search_shards";
        if (indexReadMissingAsEmpty) {
            Request req = new SimpleRequest(GET, null, target);
            Response res = executeNotFoundAllowed(req);
            if (res.status() == HttpStatus.OK) {
                shardsJson = parseContent(res.body(), "shards");
            }
            else {
                shardsJson = Collections.emptyList();
            }
        }
        else {
            shardsJson = get(target, "shards");
        }

        return shardsJson;
    }

    public Map<String, Node> getHttpNodes(boolean allowNonHttp) {
        Map<String, Map<String, Object>> nodesData = get("_nodes/http", "nodes");
        Map<String, Node> nodes = new LinkedHashMap<String, Node>();

        for (Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            Node node = new Node(entry.getKey(), entry.getValue());
            if (allowNonHttp || (node.hasHttp() && !node.isClient())) {
                nodes.put(entry.getKey(), node);
            }
        }
        return nodes;
    }

    public List<String> getHttpClientNodes() {
        Map<String, Map<String, Object>> nodesData = get("_nodes/http", "nodes");
        List<String> nodes = new ArrayList<String>();

        for (Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            Node node = new Node(entry.getKey(), entry.getValue());
            if (node.isClient() && node.hasHttp()) {
                nodes.add(node.getInet());
            }
        }
        return nodes;
    }

    public List<String> getHttpDataNodes() {
        Map<String, Map<String, Object>> nodesData = get("_nodes/http", "nodes");
        List<String> nodes = new ArrayList<String>();

        for (Entry<String, Map<String, Object>> entry : nodesData.entrySet()) {
            Node node = new Node(entry.getKey(), entry.getValue());
            if (node.isData() && node.hasHttp()) {
                nodes.add(node.getInet());
            }
        }
        return nodes;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMapping(String query) {
        return (Map<String, Object>) get(query, null);
    }
    
    public Map<String, Object> sampleForFields(String indexAndType, Collection<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return Collections.emptyMap();
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("{ \"terminate_after\":1, \"size\":1,\n");
        // use source since some fields might be objects
        sb.append("\"_source\": [");
        for (String field : fields) {
            sb.append(String.format(Locale.ROOT, "\"%s\",", field));
        }
        // remove trailing ,
        sb.setLength(sb.length() - 1);
        sb.append("],\n\"query\":{");

        if (isES20) {
            sb.append("\"bool\": { \"must\":[");
        }
        else {
            sb.append("\"constant_score\":{ \"filter\": { \"and\":[");

        }
        for (String field: fields) {
            sb.append(String.format(Locale.ROOT, "\n{ \"exists\":{ \"field\":\"%s\"} },", field));
        }
        // remove trailing ,
        sb.setLength(sb.length() - 1);
        sb.append("\n]}");

        if (!isES20) {
            sb.append("}");
        }

        sb.append("}}");
        
        Map<String, List<Map<String, Object>>> hits = parseContent(execute(GET, indexAndType + "/_search", new BytesArray(sb.toString())).body(), "hits");
        List<Map<String, Object>> docs = hits.get("hits");
        if (docs == null || docs.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> foundFields = docs.get(0);
        Map<String, Object> fieldInfo = (Map<String, Object>) foundFields.get("_source");
        
        return fieldInfo;
    }

    @Override
    public void close() {
        if (network != null) {
            network.close();
            stats.aggregate(network.stats());
            network = null;
        }
    }

    protected InputStream execute(Request request) {
        return execute(request, true).body();
    }

    protected InputStream execute(Method method, String path) {
        return execute(new SimpleRequest(method, null, path));
    }

    protected Response execute(Method method, String path, boolean checkStatus) {
        return execute(new SimpleRequest(method, null, path), checkStatus);
    }

    protected Response execute(Method method, String path, ByteSequence buffer) {
        return execute(new SimpleRequest(method, null, path, null, buffer), true);
    }

    protected Response execute(Method method, String path, ByteSequence buffer, boolean checkStatus) {
        return execute(new SimpleRequest(method, null, path, null, buffer), checkStatus);
    }

    protected Response execute(Request request, boolean checkStatus) {
        Response response = network.execute(request);
        if (checkStatus) {
            checkResponse(request, response);
        }
        return response;
    }

    protected Response executeNotFoundAllowed(Request req) {
        Response res = execute(req, false);
        switch (res.status()) {
        case HttpStatus.OK:
            break;
        case HttpStatus.NOT_FOUND:
            break;
        default:
            checkResponse(req, res);
        }

        return res;
    }

    private void checkResponse(Request request, Response response) {
        if (response.hasFailed()) {
            // check error first
            String msg = null;
            // try to parse the answer
            try {
                msg = extractError(this.<Map> parseContent(response.body(), null));
                if (response.isClientError()) {
                    msg = msg + "\n" + request.body();
                }
                else {
                    msg = prettify(msg, request.body());
                }
            } catch (Exception ex) {
                // can't parse message, move on
            }

            if (!StringUtils.hasText(msg)) {
                msg = String.format("[%s] on [%s] failed; server[%s] returned [%s|%s:%s]", request.method().name(),
                        request.path(), response.uri(), response.status(), response.statusDescription(),
                        IOUtils.asStringAlways(response.body()));
            }

            throw new EsHadoopInvalidRequest(msg);
        }
    }

    public String[] scan(String query, BytesArray body) {
        Map<String, Object> scan = parseContent(execute(POST, query, body).body(), null);

        String[] data = new String[2];
        data[0] = scan.get("_scroll_id").toString();
        data[1] = ((Map<?, ?>) scan.get("hits")).get("total").toString();
        return data;
    }

    public InputStream scroll(String scrollId) {
        // NB: dynamically get the stats since the transport can change
        long start = network.transportStats().netTotalTime;
        try {
            // use post instead of get to avoid some weird encoding issues (caused by the long URL)
            InputStream is = execute(POST, "_search/scroll?scroll=" + scrollKeepAlive.toString(),
                    new BytesArray(scrollId)).body();
            stats.scrollTotal++;
            return is;
        } finally {
            stats.scrollTotalTime += network.transportStats().netTotalTime - start;
        }
    }

    public boolean delete(String indexOrType) {
        Request req = new SimpleRequest(DELETE, null, indexOrType);
        Response res = executeNotFoundAllowed(req);
        return (res.status() == HttpStatus.OK ? true : false);
    }
    public boolean deleteScroll(String scrollId) {
        Request req = new SimpleRequest(DELETE, null, "_search/scroll", new BytesArray(scrollId.getBytes(StringUtils.UTF_8)));
        Response res = executeNotFoundAllowed(req);
        return (res.status() == HttpStatus.OK ? true : false);
    }

    public boolean exists(String indexOrType) {
        Request req = new SimpleRequest(HEAD, null, indexOrType);
        Response res = executeNotFoundAllowed(req);

        return (res.status() == HttpStatus.OK ? true : false);
    }

    public boolean touch(String indexOrType) {
        if (!exists(indexOrType)) {
            Response response = execute(PUT, indexOrType, false);

            if (response.hasFailed()) {
                String msg = null;
                // try to parse the answer
                try {
                    msg = parseContent(response.body(), "error");
                } catch (Exception ex) {
                    // can't parse message, move on
                }

                if (StringUtils.hasText(msg) && !msg.contains("IndexAlreadyExistsException")) {
                    throw new EsHadoopIllegalStateException(msg);
                }
            }
            return response.hasSucceeded();
        }
        return false;
    }

    public long count(String indexAndType, ByteSequence query) {
        Response response = execute(GET, indexAndType + "/_count", query);
        Number count = (Number) parseContent(response.body(), "count");
        return (count != null ? count.longValue() : -1);
    }

    public boolean isAlias(String query) {
        Map<String, Object> aliases = (Map<String, Object>) get(query, null);
        return (aliases.size() > 1);
    }

    public void putMapping(String index, String mapping, byte[] bytes) {
        // create index first (if needed) - it might return 403
        touch(index);

        execute(PUT, mapping, new BytesArray(bytes));
    }

    public String esVersion() {
        Map<String, String> version = get("", "version");
        if (version == null || !StringUtils.hasText(version.get("number"))) {
            return "Unknown";
        }
        return version.get("number");
    }

    public boolean health(String index, HEALTH health, TimeValue timeout) {
        StringBuilder sb = new StringBuilder("/_cluster/health/");
        sb.append(index);
        sb.append("?wait_for_status=");
        sb.append(health.name().toLowerCase(Locale.ROOT));
        sb.append("&timeout=");
        sb.append(timeout.toString());

        return (Boolean.TRUE.equals(get(sb.toString(), "timed_out")));
    }

    @Override
    public Stats stats() {
        Stats copy = new Stats(stats);
        if (network != null) {
            copy.aggregate(network.stats());
        }
        return copy;
    }

    private void countStreamStats(InputStream content) {
        if (content instanceof StatsAware) {
            stats.aggregate(((StatsAware) content).stats());
        }
    }

    public String getCurrentNode() {
        return network.currentNode();
    }
}