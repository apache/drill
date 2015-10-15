/**
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
package org.apache.drill.common.logical;

import java.util.Set;

import org.apache.drill.common.scanner.persistence.ScanResult;


public abstract class FormatPluginConfigBase implements FormatPluginConfig{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatPluginConfigBase.class);


  /**
   * scan for implementations of {@see FormatPlugin}.
   *
   * @param classpathScan - Drill configuration object, used to find the packages to scan
   * @return - list of classes that implement the interface.
   */
  public static Set<Class<? extends FormatPluginConfig>> getSubTypes(final ScanResult classpathScan) {
    final Set<Class<? extends FormatPluginConfig>> pluginClasses = classpathScan.getImplementations(FormatPluginConfig.class);
    logger.debug("Found {} format plugin configuration classes: {}.", pluginClasses.size(), pluginClasses);
    return pluginClasses;
  }

  @Override
  public abstract boolean equals(Object o);

}
