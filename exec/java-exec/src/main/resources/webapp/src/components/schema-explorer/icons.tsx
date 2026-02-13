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
import React from 'react';
import {
  DatabaseOutlined,
  FolderOutlined,
  FileOutlined,
  CloudOutlined,
  ApiOutlined,
  ConsoleSqlOutlined,
  ClusterOutlined,
  FieldNumberOutlined,
  FieldStringOutlined,
  FieldTimeOutlined,
  FieldBinaryOutlined,
  ApartmentOutlined,
  TableOutlined,
} from '@ant-design/icons';
import {
  SiSplunk,
  SiMongodb,
  SiElasticsearch,
  SiApachekafka,
  SiAmazons3,
  SiGooglecloud,
  SiApachehive,
  SiMysql,
  SiPostgresql,
  SiOracle,
  SiJson,
} from 'react-icons/si';
import { VscFilePdf, VscFileCode } from 'react-icons/vsc';
import { FaFileExcel } from 'react-icons/fa';

/** Returns an icon element for a storage plugin based on its type and name. */
export function getPluginIcon(pluginType: string, pluginName: string): React.ReactNode {
  const type = pluginType?.toLowerCase() || '';
  const name = pluginName?.toLowerCase() || '';

  // Splunk
  if (type.includes('splunk') || name.includes('splunk')) {
    return <SiSplunk style={{ color: '#65a637' }} />;
  }

  // MongoDB
  if (type.includes('mongo') || name.includes('mongo')) {
    return <SiMongodb style={{ color: '#47a248' }} />;
  }

  // Elasticsearch
  if (type.includes('elastic') || name.includes('elastic')) {
    return <SiElasticsearch style={{ color: '#005571' }} />;
  }

  // Kafka
  if (type.includes('kafka') || name.includes('kafka')) {
    return <SiApachekafka style={{ color: '#231f20' }} />;
  }

  // S3 / MinIO
  if (type.includes('s3') || name.includes('s3') || name.includes('minio')) {
    return <SiAmazons3 style={{ color: '#569a31' }} />;
  }

  // Google Cloud Storage
  if (type.includes('gcs') || type.includes('google') || name.includes('gcs') || name.includes('google')) {
    return <SiGooglecloud style={{ color: '#4285f4' }} />;
  }

  // Azure
  if (type.includes('azure') || name.includes('azure')) {
    return <CloudOutlined style={{ color: '#0078d4' }} />;
  }

  // Generic cloud
  if (type.includes('cloud')) {
    return <CloudOutlined style={{ color: '#13c2c2' }} />;
  }

  // Hive
  if (type.includes('hive') || name.includes('hive')) {
    return <SiApachehive style={{ color: '#fdee21' }} />;
  }

  // HBase / Hadoop / HDFS
  if (type.includes('hbase') || type.includes('hdfs') || name.includes('hbase') || name.includes('hdfs') || name.includes('hadoop')) {
    return <ClusterOutlined style={{ color: '#f09800' }} />;
  }

  // MySQL
  if (name.includes('mysql')) {
    return <SiMysql style={{ color: '#4479a1' }} />;
  }

  // PostgreSQL
  if (name.includes('postgres') || name.includes('pg')) {
    return <SiPostgresql style={{ color: '#4169e1' }} />;
  }

  // Oracle
  if (name.includes('oracle')) {
    return <SiOracle style={{ color: '#f80000' }} />;
  }

  // HTTP / REST API
  if (type.includes('http') || name === 'http' || name.includes('api')) {
    return <ApiOutlined style={{ color: '#fa8c16' }} />;
  }

  // File system (dfs, local)
  if (type.includes('file') || name === 'dfs' || name === 'local' || name.includes('tmp')) {
    return <FolderOutlined style={{ color: '#1890ff' }} />;
  }

  // JDBC / RDBMS (generic)
  if (type.includes('jdbc') || type.includes('rdbms')) {
    return <ConsoleSqlOutlined style={{ color: '#722ed1' }} />;
  }

  // Default
  return <DatabaseOutlined style={{ color: 'var(--color-text-secondary)' }} />;
}

/** Returns an icon element for a file based on its filename / extension. */
export function getFileIcon(filename: string): React.ReactNode {
  const ext = filename.split('.').pop()?.toLowerCase() || '';

  switch (ext) {
    case 'pdf':
      return <VscFilePdf style={{ color: '#e5252a' }} />;

    case 'csv':
    case 'tsv':
      return <VscFileCode style={{ color: '#4caf50' }} />;

    case 'json':
      return <SiJson style={{ color: 'var(--color-text)' }} />;

    case 'parquet':
      return <FileOutlined style={{ color: '#50b848' }} />;

    case 'avro':
      return <FileOutlined style={{ color: '#0d7cbf' }} />;

    case 'orc':
      return <FileOutlined style={{ color: '#ff6600' }} />;

    case 'xml':
    case 'html':
      return <VscFileCode style={{ color: '#e44d26' }} />;

    case 'xls':
    case 'xlsx':
      return <FaFileExcel style={{ color: '#217346' }} />;

    case 'log':
    case 'txt':
      return <FileOutlined style={{ color: '#8c8c8c' }} />;

    default:
      return <FileOutlined style={{ color: '#52c41a' }} />;
  }
}

/** Returns an icon element for a column based on its SQL data type. */
export function getColumnIcon(dataType: string): React.ReactNode {
  const type = dataType?.toUpperCase() || '';
  if (isComplexColumnType(dataType)) {
    return <ApartmentOutlined style={{ color: '#fa8c16' }} />;
  }
  if (type.includes('INT') || type.includes('FLOAT') || type.includes('DOUBLE') || type.includes('DECIMAL') || type.includes('NUMERIC')) {
    return <FieldNumberOutlined style={{ color: '#52c41a' }} />;
  }
  if (type.includes('DATE') || type.includes('TIME') || type.includes('TIMESTAMP')) {
    return <FieldTimeOutlined style={{ color: '#722ed1' }} />;
  }
  if (type.includes('BINARY') || type.includes('BLOB') || type.includes('BYTES')) {
    return <FieldBinaryOutlined style={{ color: '#eb2f96' }} />;
  }
  return <FieldStringOutlined style={{ color: '#1890ff' }} />;
}

/** Check if a column type represents a complex/nested structure (MAP, STRUCT, DICT). */
export function isComplexColumnType(dataType: string): boolean {
  const type = dataType?.toUpperCase() || '';
  return type === 'MAP' || type === 'STRUCT' || type === 'DICT' ||
         type === 'REPEATED_MAP' ||
         type.startsWith('STRUCT<') || type.startsWith('MAP<');
}

/** Configuration for file formats that contain multiple internal tables. */
export interface MultiTableConfig {
  formatType: string;
  paramName: string;
}

const MULTI_TABLE_FORMATS: Record<string, MultiTableConfig> = {
  xlsx: { formatType: 'excel', paramName: 'sheetName' },
  xls: { formatType: 'excel', paramName: 'sheetName' },
  h5: { formatType: 'hdf5', paramName: 'defaultPath' },
  hdf5: { formatType: 'hdf5', paramName: 'defaultPath' },
  mdb: { formatType: 'msaccess', paramName: 'tableName' },
  accdb: { formatType: 'msaccess', paramName: 'tableName' },
};

/** Check if a file has a multi-table format (Excel, HDF5, MS Access). */
export function isMultiTableFile(filename: string): boolean {
  const ext = filename.split('.').pop()?.toLowerCase() || '';
  return ext in MULTI_TABLE_FORMATS;
}

/** Get the multi-table config for a file, or undefined if not a multi-table format. */
export function getMultiTableConfig(filename: string): MultiTableConfig | undefined {
  const ext = filename.split('.').pop()?.toLowerCase() || '';
  return MULTI_TABLE_FORMATS[ext];
}

/** Returns an icon element for a sub-table (sheet, dataset, table within a file). */
export function getSubTableIcon(): React.ReactNode {
  return <TableOutlined style={{ color: '#fa8c16' }} />;
}

/** Check if a plugin is file-based (can browse files/folders). */
export function isFileBasedPlugin(pluginType: string, pluginName: string): boolean {
  const type = pluginType?.toLowerCase() || '';
  const name = pluginName?.toLowerCase() || '';
  return type.includes('file') || name === 'dfs' || name === 'local' || name.includes('tmp') ||
         type.includes('s3') || name.includes('s3') || name.includes('minio') ||
         type.includes('gcs') || type.includes('google') || name.includes('gcs') ||
         type.includes('azure') || name.includes('azure') ||
         type.includes('hdfs') || name.includes('hdfs');
}
