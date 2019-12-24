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
package org.elasticsearch.hadoop.cfg;

import org.elasticsearch.hadoop.serialization.field.DateIndexFormatter;
import org.elasticsearch.hadoop.serialization.field.DefaultIndexExtractor;
import org.elasticsearch.hadoop.serialization.field.DefaultParamsExtractor;

/**
 * Class providing the various Configuration parameters used by the Elasticsearch Hadoop integration.
 */
public interface ConfigurationOptions {

    /** Elasticsearch host **/
    // deprecated
    @Deprecated
    String ES_HOST = "es.host";
    String ES_NODES = "es.nodes";
    String ES_NODES_DEFAULT = "localhost";

    String ES_NODES_DISCOVERY = "es.nodes.discovery";
    String ES_NODES_DISCOVERY_DEFAULT = "true";

    /** Elasticsearch port **/
    String ES_PORT = "es.port";
    String ES_PORT_DEFAULT = "9200";

    /** Elasticsearch prefix **/
    String ES_NODES_PATH_PREFIX = "es.nodes.path.prefix";
    String ES_NODES_PATH_PREFIX_DEFAULT = "";

    /** Elasticsearch index */
    String ES_RESOURCE = "es.resource";
    String ES_RESOURCE_READ = "es.resource.read";
    String ES_RESOURCE_WRITE = "es.resource.write";

    String ES_RTQUERY_DATABASES= "es.rtquery.databases";

    String ES_QUERY = "es.query";

    /** Clients only */
    String ES_NODES_CLIENT_ONLY = "es.nodes.client.only";
    String ES_NODES_CLIENT_ONLY_DEFAULT = "false";

    /** Data only */
    String ES_NODES_DATA_ONLY = "es.nodes.data.only";
    String ES_NODES_DATA_ONLY_DEFAULT = "true";

    /** WAN only */
    String ES_NODES_WAN_ONLY = "es.nodes.wan.only";
    String ES_NODES_WAN_ONLY_DEFAULT = "false";


    /**  multiple indice skip powerlist */
    String ES_MULTIPLE_INDICE_SKIP_POWERLIST = "es_multiple_indice_skip_powerlist";
    String ES_MULTIPLE_INDICE_SKIP_POWERLIST_DEFAULT = "true";

    /** Elasticsearch batch size given in bytes */
    String ES_BATCH_SIZE_BYTES = "es.batch.size.bytes";
    String ES_BATCH_SIZE_BYTES_DEFAULT = "1mb";

    /** Elasticsearch batch size given in entries */
    String ES_BATCH_SIZE_ENTRIES = "es.batch.size.entries";
    String ES_BATCH_SIZE_ENTRIES_DEFAULT = "1000";

    /** Elasticsearch disable auto-flush on batch overflow */
    String ES_BATCH_FLUSH_MANUAL = "es.batch.flush.manual";
    String ES_BATCH_FLUSH_MANUAL_DEFAULT = "false";

    /** Whether to trigger an index refresh after doing batch writing */
    String ES_BATCH_WRITE_REFRESH = "es.batch.write.refresh";
    String ES_BATCH_WRITE_REFRESH_DEFAULT = "true";

    /** HTTP bulk retries **/
    String ES_BATCH_WRITE_RETRY_COUNT = "es.batch.write.retry.count";
    String ES_BATCH_WRITE_RETRY_COUNT_DEFAULT = "3";

    String ES_BATCH_WRITE_RETRY_WAIT = "es.batch.write.retry.wait";
    String ES_BATCH_WRITE_RETRY_WAIT_DEFAULT = "10s";

    String ES_BATCH_WRITE_RETRY_POLICY = "es.batch.write.retry.policy";
    String ES_BATCH_WRITE_RETRY_POLICY_NONE = "none";
    String ES_BATCH_WRITE_RETRY_POLICY_SIMPLE = "simple";
    String ES_BATCH_WRITE_RETRY_POLICY_DEFAULT = ES_BATCH_WRITE_RETRY_POLICY_SIMPLE;

    /** HTTP connection timeout */
    String ES_HTTP_TIMEOUT = "es.http.timeout";
    String ES_HTTP_TIMEOUT_DEFAULT = "1m";

    String ES_HTTP_RETRIES = "es.http.retries";
    String ES_HTTP_RETRIES_DEFAULT = "3";

    /** Scroll keep-alive */
    String ES_SCROLL_KEEPALIVE = "es.scroll.keepalive";
    String ES_SCROLL_KEEPALIVE_DEFAULT = "5m";

    /** Scroll size */
    String ES_SCROLL_SIZE = "es.scroll.size";
    String ES_SCROLL_SIZE_DEFAULT = "50";

    /** Scroll limit */
    String ES_SCROLL_LIMIT = "es.scroll.limit";
    String ES_SCROLL_LIMIT_DEFAULT = "-1";

    /** Scroll fields */

    @Deprecated
    String ES_SCROLL_ESCAPE_QUERY_URI = "es.scroll.escape.query.uri";
    @Deprecated
    String ES_SCROLL_ESCAPE_QUERY_URI_DEFAULT = "true";

    String ES_HEART_BEAT_LEAD = "es.action.heart.beat.lead";
    String ES_HEART_BEAT_LEAD_DEFAULT = "15s";

    /** Serialization settings */

    /** Value writer - setup automatically; can be overridden for custom types */
    String ES_SERIALIZATION_WRITER_VALUE_CLASS = "es.ser.writer.value.class";

    /** JSON/Bytes writer - setup automatically; can be overridden for custom types */
    String ES_SERIALIZATION_WRITER_BYTES_CLASS = "es.ser.writer.bytes.class";

    /** Value reader - setup automatically; can be overridden for custom types */
    String ES_SERIALIZATION_READER_VALUE_CLASS = "es.ser.reader.value.class";

    /** Input options **/
    String ES_INPUT_JSON = "es.input.json";
    String ES_INPUT_JSON_DEFAULT = "no";

    /** Index settings */
    String ES_INDEX_AUTO_CREATE = "es.index.auto.create";
    String ES_INDEX_AUTO_CREATE_DEFAULT = "yes";

    String ES_INDEX_READ_MISSING_AS_EMPTY = "es.index.read.missing.as.empty";
    String ES_INDEX_READ_MISSING_AS_EMPTY_DEFAULT = "false";

    /** Mapping types */
    String ES_MAPPING_DEFAULT_EXTRACTOR_CLASS = "es.mapping.default.extractor.class";

    String ES_MAPPING_ID = "es.mapping.id";
    String ES_MAPPING_ID_EXTRACTOR_CLASS = "es.mapping.id.extractor.class";
    String ES_MAPPING_PARENT = "es.mapping.parent";
    String ES_MAPPING_PARENT_EXTRACTOR_CLASS = "es.mapping.parent.extractor.class";
    String ES_MAPPING_VERSION = "es.mapping.version";
    String ES_MAPPING_VERSION_EXTRACTOR_CLASS = "es.mapping.version.extractor.class";
    String ES_MAPPING_ROUTING = "es.mapping.routing";
    String ES_MAPPING_ROUTING_EXTRACTOR_CLASS = "es.mapping.routing.extractor.class";
    String ES_MAPPING_TTL = "es.mapping.ttl";
    String ES_MAPPING_TTL_EXTRACTOR_CLASS = "es.mapping.ttl.extractor.class";
    String ES_MAPPING_TIMESTAMP = "es.mapping.timestamp";
    String ES_MAPPING_TIMESTAMP_EXTRACTOR_CLASS = "es.mapping.timestamp.extractor.class";
    String ES_MAPPING_INDEX_EXTRACTOR_CLASS = "es.mapping.index.extractor.class";
    String ES_MAPPING_DEFAULT_INDEX_EXTRACTOR_CLASS = DefaultIndexExtractor.class.getName();
    String ES_MAPPING_INDEX_FORMATTER_CLASS = "es.mapping.index.formatter.class";
    String ES_MAPPING_DEFAULT_INDEX_FORMATTER_CLASS = DateIndexFormatter.class.getName();
    String ES_MAPPING_PARAMS_EXTRACTOR_CLASS = "es.mapping.params.extractor.class";
    String ES_MAPPING_PARAMS_DEFAULT_EXTRACTOR_CLASS = DefaultParamsExtractor.class.getName();
    String ES_MAPPING_CONSTANT_AUTO_QUOTE = "es.mapping.constant.auto.quote";
    String ES_MAPPING_CONSTANT_AUTO_QUOTE_DEFAULT = "true";

    String ES_MAPPING_DATE_RICH_OBJECT = "es.mapping.date.rich";
    String ES_MAPPING_DATE_RICH_OBJECT_DEFAULT = "true";


    String ES_MAPPING_VERSION_TYPE = "es.mapping.version.type";
    String ES_MAPPING_VERSION_TYPE_INTERNAL = "internal";
    String ES_MAPPING_VERSION_TYPE_EXTERNAL = "external";
    String ES_MAPPING_VERSION_TYPE_EXTERNAL_GT = "external_gt";
    String ES_MAPPING_VERSION_TYPE_EXTERNAL_GTE = "external_gte";
    String ES_MAPPING_VERSION_TYPE_FORCE = "force";

    String ES_MAPPING_INCLUDE = "es.mapping.include";
    String ES_MAPPING_INCLUDE_DEFAULT = "";
    String ES_MAPPING_EXCLUDE = "es.mapping.exclude";
    String ES_MAPPING_EXCLUDE_DEFAULT = "";


    /** Read settings */

    /** Field options **/
    String ES_READ_FIELD_EMPTY_AS_NULL = "es.read.field.empty.as.null";
    String ES_READ_FIELD_EMPTY_AS_NULL_LEGACY = "es.field.read.empty.as.null";
    String ES_READ_FIELD_EMPTY_AS_NULL_DEFAULT = "yes";

    String ES_READ_FIELD_VALIDATE_PRESENCE = "es.read.field.validate.presence";
    String ES_READ_FIELD_VALIDATE_PRESENCE_LEGACY = "es.field.read.validate.presence";
    String ES_READ_FIELD_VALIDATE_PRESENCE_DEFAULT = "warn";

    String ES_READ_FIELD_INCLUDE = "es.read.field.include";
    String ES_READ_FIELD_EXCLUDE = "es.read.field.exclude";

    String ES_READ_FIELD_AS_ARRAY_INCLUDE = "es.read.field.as.array.include";
    String ES_READ_FIELD_AS_ARRAY_EXCLUDE = "es.read.field.as.array.exclude";


    /** Metadata */
    String ES_READ_METADATA = "es.read.metadata";
    String ES_READ_METADATA_DEFAULT = "false";
    String ES_READ_METADATA_FIELD = "es.read.metadata.field";
    String ES_READ_METADATA_FIELD_DEFAULT = "_metadata";
    String ES_READ_METADATA_VERSION = "es.read.metadata.version";
    String ES_READ_METADATA_VERSION_DEFAULT = "false";
    String ES_READ_UNMAPPED_FIELDS_IGNORE = "es.read.unmapped.fields.ignore";
    String ES_READ_UNMAPPED_FIELDS_IGNORE_DEFAULT = "true";


    /** Operation types */
    String ES_WRITE_OPERATION = "es.write.operation";
    String ES_OPERATION_INDEX = "index";
    String ES_OPERATION_CREATE = "create";
    String ES_OPERATION_UPDATE = "update";
    String ES_OPERATION_UPSERT = "upsert";
    String ES_OPERATION_DELETE = "delete";
    String ES_WRITE_OPERATION_DEFAULT = ES_OPERATION_INDEX;

    String ES_UPDATE_RETRY_ON_CONFLICT = "es.update.retry.on.conflict";
    String ES_UPDATE_RETRY_ON_CONFLICT_DEFAULT = "0";

    String ES_UPDATE_SCRIPT = "es.update.script";
    String ES_UPDATE_SCRIPT_LANG = "es.update.script.lang";
    String ES_UPDATE_SCRIPT_PARAMS = "es.update.script.params";
    String ES_UPDATE_SCRIPT_PARAMS_JSON = "es.update.script.params.json";

    /** Output options **/
    String ES_OUTPUT_JSON = "es.output.json";
    String ES_OUTPUT_JSON_DEFAULT = "no";

    /** Network options */
    // SSL
    String ES_NET_USE_SSL = "es.net.ssl";
    String ES_NET_USE_SSL_DEFAULT = "false";

    String ES_NET_SSL_PROTOCOL = "es.net.ssl.protocol";
    String ES_NET_SSL_PROTOCOL_DEFAULT = "TLS"; // SSL as an alternative

    String ES_NET_SSL_KEYSTORE_LOCATION = "es.net.ssl.keystore.location";
    String ES_NET_SSL_KEYSTORE_TYPE = "es.net.ssl.keystore.type";
    String ES_NET_SSL_KEYSTORE_TYPE_DEFAULT = "JKS"; // PKCS12 could also be used
    String ES_NET_SSL_KEYSTORE_PASS = "es.net.ssl.keystore.pass";

    String ES_NET_SSL_TRUST_STORE_LOCATION = "es.net.ssl.truststore.location";
    String ES_NET_SSL_TRUST_STORE_PASS = "es.net.ssl.truststore.pass";

    String ES_NET_SSL_CERT_ALLOW_SELF_SIGNED = "es.net.ssl.cert.allow.self.signed";
    String ES_NET_SSL_CERT_ALLOW_SELF_SIGNED_DEFAULT = "false";

    String ES_NET_HTTP_AUTH_USER = "es.net.http.auth.user";
    String ES_NET_HTTP_AUTH_PASS = "es.net.http.auth.pass";

    String ES_NET_PROXY_HTTP_HOST = "es.net.proxy.http.host";
    String ES_NET_PROXY_HTTP_PORT = "es.net.proxy.http.port";
    String ES_NET_PROXY_HTTP_USER = "es.net.proxy.http.user";
    String ES_NET_PROXY_HTTP_PASS = "es.net.proxy.http.pass";
    String ES_NET_PROXY_HTTP_USE_SYSTEM_PROPS = "es.net.proxy.http.use.system.props";
    String ES_NET_PROXY_HTTP_USE_SYSTEM_PROPS_DEFAULT = "yes";

    String ES_NET_PROXY_HTTPS_HOST = "es.net.proxy.https.host";
    String ES_NET_PROXY_HTTPS_PORT = "es.net.proxy.https.port";
    String ES_NET_PROXY_HTTPS_USER = "es.net.proxy.https.user";
    String ES_NET_PROXY_HTTPS_PASS = "es.net.proxy.https.pass";
    String ES_NET_PROXY_HTTPS_USE_SYSTEM_PROPS = "es.net.proxy.https.use.system.props";
    String ES_NET_PROXY_HTTPS_USE_SYSTEM_PROPS_DEFAULT = "yes";

    String ES_NET_PROXY_SOCKS_HOST = "es.net.proxy.socks.host";
    String ES_NET_PROXY_SOCKS_PORT = "es.net.proxy.socks.port";
    String ES_NET_PROXY_SOCKS_USER = "es.net.proxy.socks.user";
    String ES_NET_PROXY_SOCKS_PASS = "es.net.proxy.socks.pass";
    String ES_NET_PROXY_SOCKS_USE_SYSTEM_PROPS = "es.net.proxy.socks.use.system.props";
    String ES_NET_PROXY_SOCKS_USE_SYSTEM_PROPS_DEFAULT = "yes";
}
