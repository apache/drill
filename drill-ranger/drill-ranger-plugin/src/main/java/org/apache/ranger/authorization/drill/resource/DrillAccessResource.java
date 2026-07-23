/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.authorization.drill.resource;

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class DrillAccessResource extends RangerAccessResourceImpl {

  private static final Logger LOG = LoggerFactory.getLogger(DrillAccessResource.class);

  public DrillAccessResource() {
  }

  public DrillAccessResource(Map<RangerDrillResource, Optional<String>> resource) {
    super();
    for (Map.Entry<RangerDrillResource, Optional<String>> entry : resource.entrySet()) {
      String key = entry.getKey().toString();
      Optional<String> value = entry.getValue();
      value.ifPresent(s -> this.setValue(key, s));
      if (LOG.isDebugEnabled()) {
        LOG.debug("AccessResource set value: {} = {}", key, value);
      }
    }
  }

  public DrillAccessResource(String catalogName, Optional<String> schema, Optional<String> table) {
    setValue(RangerDrillResource.DATASOURCE.toString(), catalogName);
    schema.ifPresent(s -> setValue(RangerDrillResource.SCHEMA.toString(), s));
    table.ifPresent(s -> setValue(RangerDrillResource.TABLE.toString(), s));
  }

  public DrillAccessResource(String catalogName, Optional<String> schema, Optional<String> table,
      Optional<String> column) {
    setValue(RangerDrillResource.DATASOURCE.toString(), catalogName);
    schema.ifPresent(s -> setValue(RangerDrillResource.SCHEMA.toString(), s));
    table.ifPresent(s -> setValue(RangerDrillResource.TABLE.toString(), s));
    column.ifPresent(s -> setValue(RangerDrillResource.COLUMN.toString(), s));
  }

  public String getCatalogName() {
    return (String) getValue(RangerDrillResource.DATASOURCE.toString());
  }

  public String getTable() {
    return (String) getValue(RangerDrillResource.TABLE.toString());
  }

  public String getCatalog() {
    return (String) getValue(RangerDrillResource.SCHEMA.toString());
  }

  public String getSchema() {
    return (String) getValue(RangerDrillResource.SCHEMA.toString());
  }


}

enum RangerDrillResource {
  DATASOURCE("datasource"),
  SCHEMA("schema"),
  TABLE("table"),
  COLUMN("column");

  private final String key;

  RangerDrillResource(String key) {
    this.key = key;
  }

  @Override
  public String toString() {
    return key;
  }
}
