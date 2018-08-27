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
package org.apache.drill.exec.store.dfs;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.FormatPluginConfigBase;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.TableInstance;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.TableSignature;
import org.slf4j.Logger;

import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;

/**
 * manages format plugins options to define table macros
 */
final class FormatPluginOptionExtractor {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(FormatPluginOptionExtractor.class);

  private final Map<String, FormatPluginOptionsDescriptor> optionsByTypeName;

  /**
   * extracts the format plugin options based on the scanned implementations of {@link FormatPluginConfig}
   * @param scanResult
   */
  FormatPluginOptionExtractor(ScanResult scanResult) {
    Map<String, FormatPluginOptionsDescriptor> result = new HashMap<>();
    Set<Class<? extends FormatPluginConfig>> pluginConfigClasses = FormatPluginConfigBase.getSubTypes(scanResult);
    for (Class<? extends FormatPluginConfig> pluginConfigClass : pluginConfigClasses) {
      FormatPluginOptionsDescriptor optionsDescriptor = new FormatPluginOptionsDescriptor(pluginConfigClass);
      result.put(optionsDescriptor.typeName.toLowerCase(), optionsDescriptor);
    }
    this.optionsByTypeName = unmodifiableMap(result);
  }

  /**
   * @return the extracted options
   */
  @VisibleForTesting
  Collection<FormatPluginOptionsDescriptor> getOptions() {
    return optionsByTypeName.values();
  }

  /**
   * give a table name, returns function signatures to configure the FormatPlugin
   * @param tableName the name of the table (or table function in this context)
   * @return the available signatures
   */
  List<TableSignature> getTableSignatures(String tableName) {
    List<TableSignature> result = new ArrayList<>();
    for (FormatPluginOptionsDescriptor optionsDescriptor : optionsByTypeName.values()) {
      TableSignature sig = optionsDescriptor.getTableSignature(tableName);
      result.add(sig);
    }
    return unmodifiableList(result);
  }

  /**
   * given a table function signature and the corresponding parameters
   * return the corresponding formatPlugin configuration
   * @param t the signature and parameters (it should be one of the signatures returned by {@link FormatPluginOptionExtractor#getTableSignatures(String)})
   * @return the config
   */
  FormatPluginConfig createConfigForTable(TableInstance t) {
    if (!t.sig.params.get(0).name.equals("type")) {
      throw UserException.parseError()
        .message("unknown first param for %s", t.sig)
        .addContext("table", t.sig.name)
        .build(logger);
    }
    String type = (String)t.params.get(0);
    if (type == null) {
      throw UserException.parseError()
          .message("type param must be present but was missing")
          .addContext("table", t.sig.name)
          .build(logger);
    }
    FormatPluginOptionsDescriptor optionsDescriptor = optionsByTypeName.get(type.toLowerCase());
    if (optionsDescriptor == null) {
      throw UserException.parseError()
          .message(
              "unknown type %s, expected one of %s",
              type, optionsByTypeName.keySet())
          .addContext("table", t.sig.name)
          .build(logger);
    }
    return optionsDescriptor.createConfigForTable(t);
  }
}