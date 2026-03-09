/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { PluginType } from '../../types';

export const pluginTemplates: Record<string, Record<string, unknown>> = {
  file: {
    type: 'file',
    connection: 'file:///',
    workspaces: {
      root: {
        location: '/',
        writable: false,
      },
    },
    formats: {
      csv: {
        type: 'text',
        extensions: ['csv'],
        delimiter: ',',
      },
      tsv: {
        type: 'text',
        extensions: ['tsv'],
        delimiter: '\t',
      },
      json: {
        type: 'json',
        extensions: ['json'],
      },
      parquet: {
        type: 'parquet',
      },
    },
    enabled: false,
  },
  jdbc: {
    type: 'jdbc',
    driver: '',
    url: '',
    username: '',
    password: '',
    writable: false,
    enabled: false,
  },
  http: {
    type: 'http',
    connections: {},
    cacheResults: false,
    timeout: 0,
    enabled: false,
  },
  mongo: {
    type: 'mongo',
    connection: 'mongodb://localhost:27017',
    batchSize: 100,
    enabled: false,
  },
  cassandra: {
    type: 'cassandra',
    host: 'localhost',
    port: 9042,
    username: '',
    password: '',
    enabled: false,
  },
  druid: {
    type: 'druid',
    brokerAddress: 'http://localhost:8082',
    coordinatorAddress: 'http://localhost:8081',
    averageRowSizeBytes: 100,
    enabled: false,
  },
  hbase: {
    type: 'hbase',
    config: {
      'hbase.zookeeper.quorum': 'localhost',
      'hbase.zookeeper.property.clientPort': '2181',
    },
    'size.calculator.enabled': false,
    enabled: false,
  },
  kafka: {
    type: 'kafka',
    kafkaConsumerProps: {
      'bootstrap.servers': 'localhost:9092',
    },
    enabled: false,
  },
  hive: {
    type: 'hive',
    configProps: {
      'hive.metastore.uris': '',
    },
    enabled: false,
  },
  elastic: {
    type: 'elastic',
    hosts: ['http://localhost:9200'],
    username: '',
    password: '',
    enabled: false,
  },
  kudu: {
    type: 'kudu',
    masterAddresses: 'localhost:7051',
    enabled: false,
  },
  openTSDB: {
    type: 'openTSDB',
    connection: 'http://localhost:4242',
    enabled: false,
  },
  phoenix: {
    type: 'phoenix',
    zkQuorum: 'localhost',
    port: 2181,
    zkPath: '/hbase',
    enabled: false,
  },
  splunk: {
    type: 'splunk',
    username: 'admin',
    password: '',
    scheme: 'https',
    hostname: 'localhost',
    port: 8089,
    earliestTime: '-14d',
    latestTime: 'now',
    reconnectRetries: 3,
    enabled: false,
  },
};

export function getTemplate(type: PluginType): Record<string, unknown> {
  return pluginTemplates[type]
    ? JSON.parse(JSON.stringify(pluginTemplates[type]))
    : { type: type || '', enabled: false };
}

/**
 * Map of plugin type identifiers to logo filenames.
 * Logo images are served from /static/img/storage_logos/.
 */
export const pluginLogos: Record<string, string> = {
  file: 'FileSystem.png',
  jdbc: 'Jdbc.png',
  http: 'Http.png',
  mongo: 'Mongo.png',
  hive: 'Hive.png',
  hbase: 'HBase.png',
  kafka: 'Kafka.png',
  elasticsearch: 'Elasticsearch.png',
  druid: 'Druid.png',
  splunk: 'Splunk.png',
  kudu: 'Kudu.png',
  phoenix: 'Phoenix.png',
  opentsdb: 'OpenTSDB.png',
  cassandra: 'Cassandra.png',
};

/**
 * Logos for file-system sub-types, keyed by connection protocol prefix.
 * Checked before the generic `file` logo when a connection string is provided.
 */
const fileSystemSubLogos: { prefix: string; logo: string }[] = [
  { prefix: 's3a://', logo: 'S3.png' },
  { prefix: 'gs://', logo: 'GCP.png' },
  { prefix: 'hdfs://', logo: 'Hadoop.png' },
  { prefix: 'wasbs://', logo: 'Azure.png' },
  { prefix: 'wasb://', logo: 'Azure.png' },
  { prefix: 'abfss://', logo: 'Azure.png' },
  { prefix: 'abfs://', logo: 'Azure.png' },
  { prefix: 'oci://', logo: 'OCI.png' },
  { prefix: 'sftp://', logo: 'SFTP.png' },
  { prefix: 'dropbox:///', logo: 'Dropbox.png' },
  { prefix: 'box:///', logo: 'Box.png' },
];

/**
 * Get the logo URL for a given plugin type.
 * For `file` type plugins, pass the connection string to resolve sub-type logos
 * (e.g. S3).  Returns undefined if no logo is available.
 */
export function getPluginLogoUrl(type: string, connection?: string): string | undefined {
  const lower = type.toLowerCase();
  // For file-system plugins, check connection-specific logos first
  if (lower === 'file' && connection) {
    const connLower = connection.toLowerCase();
    for (const entry of fileSystemSubLogos) {
      if (connLower.startsWith(entry.prefix)) {
        return `/static/img/storage_logos/${entry.logo}`;
      }
    }
  }
  const filename = pluginLogos[lower];
  if (filename) {
    return `/static/img/storage_logos/${filename}`;
  }
  return undefined;
}

/** Gradient colors per plugin type for card covers */
export const pluginGradients: Record<string, string> = {
  file: 'linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)',
  jdbc: 'linear-gradient(135deg, #13c2c2 0%, #006d75 100%)',
  http: 'linear-gradient(135deg, #52c41a 0%, #237804 100%)',
  mongo: 'linear-gradient(135deg, #389e0d 0%, #135200 100%)',
  hive: 'linear-gradient(135deg, #faad14 0%, #ad6800 100%)',
  hbase: 'linear-gradient(135deg, #eb2f96 0%, #9e1068 100%)',
  kafka: 'linear-gradient(135deg, #2f54eb 0%, #10239e 100%)',
  elasticsearch: 'linear-gradient(135deg, #fadb14 0%, #ad8b00 100%)',
  druid: 'linear-gradient(135deg, #722ed1 0%, #391085 100%)',
  splunk: 'linear-gradient(135deg, #f5222d 0%, #a8071a 100%)',
};

export function getPluginGradient(type: string): string {
  return pluginGradients[type.toLowerCase()] || 'linear-gradient(135deg, #595959 0%, #262626 100%)';
}

/** Known plugin types for the create flow */
export const knownPluginTypes = [
  { value: 'cassandra', label: 'Cassandra / ScyllaDB' },
  { value: 'druid', label: 'Druid' },
  { value: 'elastic', label: 'Elasticsearch' },
  { value: 'file', label: 'File System' },
  { value: 'hbase', label: 'HBase' },
  { value: 'hive', label: 'Hive' },
  { value: 'http', label: 'HTTP' },
  { value: 'jdbc', label: 'JDBC' },
  { value: 'kafka', label: 'Kafka' },
  { value: 'kudu', label: 'Kudu' },
  { value: 'mongo', label: 'MongoDB' },
  { value: 'openTSDB', label: 'OpenTSDB' },
  { value: 'phoenix', label: 'Phoenix' },
  { value: 'splunk', label: 'Splunk' },
];
