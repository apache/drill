/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;


public class SecurityConfiguration extends Configuration {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SecurityConfiguration.class);

  public SecurityConfiguration() {
    super();
    updateGroupMapping();
  }

  /**
   * Update the Group Mapping class name to add namespace prefix retrieved from System Property. This is needed since
   * in drill-jdbc-all jar we are packaging hadoop dependencies under that namespace. This will help application
   * using this jar as driver to avoid conflict with it's own hadoop dependency if any.
   */
  private void updateGroupMapping() {
    final String originalClassName = get(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING);
    final String profilePrefix = System.getProperty("namespacePrefix");
    final String updatedClassName = (profilePrefix != null) ? (profilePrefix + originalClassName)
                                                            : originalClassName;
    set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING, updatedClassName);
  }
}
