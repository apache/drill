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
package org.apache.drill.exec.store.drill.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@JsonTypeName("drill-read")
public class DrillSubScan extends AbstractBase implements SubScan {

  public static final String OPERATOR_TYPE = "DRILL_SUB_SCAN";

  private final String query;

  @JsonProperty
  private final DrillStoragePluginConfig pluginConfig;

  @JsonCreator
  public DrillSubScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("pluginConfig") StoragePluginConfig pluginConfig,
      @JsonProperty("query") String query) {
    super(userName);
    this.pluginConfig = (DrillStoragePluginConfig) pluginConfig;
    this.query = query;
  }

  public DrillSubScan(String userName,
    DrillStoragePluginConfig storagePluginConfig,
    String query) {
    super(userName);
    this.pluginConfig = storagePluginConfig;
    this.query = query;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new DrillSubScan(getUserName(), pluginConfig, query);
  }

  public DrillStoragePluginConfig getPluginConfig() {
    return pluginConfig;
  }

  public String getQuery() {
    return query;
  }

  @Override
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }
}
