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

export interface FormatConfig {
  type: string;
  extensions?: string[];
  [key: string]: unknown;
}

export interface FormatMeta {
  label: string;
  description: string;
  category: 'text' | 'binary' | 'log' | 'office' | 'scientific' | 'table' | 'markup' | 'document' | 'gis' | 'database' | 'metadata' | 'streaming';
}

export const FORMAT_DEFAULTS: Record<string, FormatConfig> = {
  // Standard Text Formats (9)
  csv: {
    type: 'text',
    extensions: ['csv'],
    fieldDelimiter: ',',
    quote: '"',
    escape: '"',
  },
  tsv: {
    type: 'text',
    extensions: ['tsv'],
    fieldDelimiter: '\t',
  },
  psv: {
    type: 'text',
    extensions: ['tbl'],
    fieldDelimiter: '|',
  },
  csvh: {
    type: 'text',
    extensions: ['csvh'],
    fieldDelimiter: ',',
    extractHeader: true,
  },
  text: {
    type: 'text',
    extensions: ['txt'],
    fieldDelimiter: ',',
  },

  // Standard Binary/Data Formats
  json: {
    type: 'json',
    extensions: ['json'],
  },
  parquet: {
    type: 'parquet',
    extensions: ['parquet'],
  },
  avro: {
    type: 'avro',
    extensions: ['avro'],
  },
  sequencefile: {
    type: 'sequencefile',
    extensions: ['seq'],
  },

  // Contrib: Office Formats
  excel: {
    type: 'xlsx',
    extensions: ['xlsx', 'xls'],
    headerRow: 0,
  },

  // Contrib: Markup
  xml: {
    type: 'xml',
    extensions: ['xml'],
    dataLevel: 1,
  },

  // Contrib: Document Formats
  pdf: {
    type: 'pdf',
    extensions: ['pdf'],
  },

  // Contrib: Image Format
  image: {
    type: 'image',
    extensions: ['jpg', 'jpeg', 'png', 'gif', 'bmp'],
  },

  // Contrib: Scientific Data Formats
  hdf5: {
    type: 'hdf5',
    extensions: ['h5', 'hdf5'],
  },

  // Contrib: Log Formats
  log: {
    type: 'log',
    extensions: ['log'],
  },
  httpd: {
    type: 'httpd',
    extensions: ['log'],
  },
  syslog: {
    type: 'syslog',
    extensions: ['log'],
  },
  ltsv: {
    type: 'ltsv',
    extensions: ['ltsv'],
  },

  // Contrib: Network/GIS
  pcap: {
    type: 'pcap',
    extensions: ['pcap', 'pcapng'],
  },
  shp: {
    type: 'shp',
    extensions: ['shp'],
  },

  // Contrib: Statistical Data
  sas: {
    type: 'sas',
    extensions: ['sas7bdat'],
  },
  spss: {
    type: 'spss',
    extensions: ['sav'],
  },

  // Contrib: Database Format
  msaccess: {
    type: 'msaccess',
    extensions: ['accdb', 'mdb'],
  },

  // Contrib: Table Formats
  iceberg: {
    type: 'iceberg',
  },
  delta: {
    type: 'delta',
  },
  paimon: {
    type: 'paimon',
  },

  // Contrib: MapR Specific
  maprdb: {
    type: 'maprdb',
  },
};

export const FORMAT_METADATA: Record<string, FormatMeta> = {
  // Standard Text (9)
  csv: { label: 'CSV', description: 'Comma-separated values', category: 'text' },
  tsv: { label: 'TSV', description: 'Tab-separated values', category: 'text' },
  psv: { label: 'PSV', description: 'Pipe-separated values', category: 'text' },
  csvh: { label: 'CSV with Header', description: 'CSV with header row', category: 'text' },
  text: { label: 'Text', description: 'Plain text with delimiter', category: 'text' },
  json: { label: 'JSON', description: 'JavaScript Object Notation', category: 'text' },
  parquet: { label: 'Parquet', description: 'Apache Parquet columnar format', category: 'binary' },
  avro: { label: 'Avro', description: 'Apache Avro serialization format', category: 'binary' },
  sequencefile: { label: 'Sequence File', description: 'Hadoop sequence file format', category: 'binary' },

  // Contrib: Office (1)
  excel: { label: 'Excel', description: 'Microsoft Excel spreadsheets', category: 'office' },

  // Contrib: Markup (1)
  xml: { label: 'XML', description: 'Extensible Markup Language', category: 'markup' },

  // Contrib: Document (1)
  pdf: { label: 'PDF', description: 'Portable Document Format', category: 'document' },

  // Contrib: Image (1)
  image: { label: 'Image', description: 'Image files (JPEG, PNG, etc.)', category: 'metadata' },

  // Contrib: Scientific (1)
  hdf5: { label: 'HDF5', description: 'Hierarchical Data Format 5', category: 'scientific' },

  // Contrib: Logs (4)
  log: { label: 'Log', description: 'Generic log file format', category: 'log' },
  httpd: { label: 'Apache HTTP', description: 'Apache HTTP server log format', category: 'log' },
  syslog: { label: 'Syslog', description: 'Unix syslog format', category: 'log' },
  ltsv: { label: 'LTSV', description: 'Labeled Tab-Separated Values', category: 'log' },

  // Contrib: Network/GIS (2)
  pcap: { label: 'PCAP', description: 'Packet capture file format', category: 'gis' },
  shp: { label: 'Shapefile', description: 'ESRI Shapefile GIS format', category: 'gis' },

  // Contrib: Statistical (2)
  sas: { label: 'SAS', description: 'SAS 7 transport format', category: 'scientific' },
  spss: { label: 'SPSS', description: 'IBM SPSS statistical format', category: 'scientific' },

  // Contrib: Database (1)
  msaccess: { label: 'MS Access', description: 'Microsoft Access database', category: 'database' },

  // Contrib: Table Formats (3)
  iceberg: { label: 'Apache Iceberg', description: 'Iceberg table format', category: 'table' },
  delta: { label: 'Delta Lake', description: 'Databricks Delta Lake format', category: 'table' },
  paimon: { label: 'Apache Paimon', description: 'Apache Paimon table format', category: 'table' },

  // Contrib: MapR (1)
  maprdb: { label: 'MapR-DB', description: 'MapR-DB database format', category: 'database' },
};

export const STANDARD_FORMATS = [
  'text', 'csv', 'tsv', 'psv', 'csvh', 'json', 'parquet', 'avro', 'sequencefile'
];

export const CONTRIB_FORMATS = [
  'excel', 'xml', 'pdf', 'image', 'hdf5', 'log', 'httpd', 'syslog', 'ltsv',
  'pcap', 'shp', 'sas', 'spss', 'msaccess', 'iceberg', 'delta', 'paimon',
  'maprdb'
];
