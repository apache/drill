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

import java.util.List;

import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.PathScanner;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class StoragePluginConfigBase implements StoragePluginConfig{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginConfigBase.class);
  
  
  public synchronized static Class<?>[] getSubTypes(DrillConfig config){
    List<String> packages = config.getStringList(CommonConstants.STORAGE_PLUGIN_CONFIG_SCAN_PACKAGES);
    Class<?>[] sec = PathScanner.scanForImplementationsArr(StoragePluginConfig.class, packages);
    logger.debug("Adding Storage Engine Configs including {}", (Object) sec );
    return sec;
  }

  public abstract boolean equals(Object o);
  
}
