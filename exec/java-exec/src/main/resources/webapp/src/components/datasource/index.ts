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
export { default as FileSystemForm } from './FileSystemForm';
export { default as JdbcForm } from './JdbcForm';
export { default as HttpForm } from './HttpForm';
export { default as MongoForm } from './MongoForm';
export { default as SplunkForm } from './SplunkForm';
export { default as CassandraForm } from './CassandraForm';
export { default as DruidForm } from './DruidForm';
export { default as ElasticsearchForm } from './ElasticsearchForm';
export { default as GoogleSheetsForm } from './GoogleSheetsForm';
export { default as HBaseForm } from './HBaseForm';
export { default as HiveForm } from './HiveForm';
export { default as KafkaForm } from './KafkaForm';
export { default as KuduForm } from './KuduForm';
export { default as OpenTSDBForm } from './OpenTSDBForm';
export { default as PhoenixForm } from './PhoenixForm';
export {
  pluginTemplates,
  getTemplate,
  pluginLogos,
  getPluginLogoUrl,
  pluginGradients,
  getPluginGradient,
  knownPluginTypes,
} from './PluginTemplates';
