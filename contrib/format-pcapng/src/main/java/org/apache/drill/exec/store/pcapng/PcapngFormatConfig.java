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
package org.apache.drill.exec.store.pcapng;

import java.util.List;
import java.util.Objects;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(PcapngFormatConfig.NAME)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class PcapngFormatConfig implements FormatPluginConfig {

  public static final String NAME = "pcapng";
  private final List<String> extensions;
  private final boolean stat;

  @JsonCreator
  public PcapngFormatConfig(@JsonProperty("extensions") List<String> extensions, @JsonProperty("stat") boolean stat) {
    this.extensions = extensions == null ? ImmutableList.of(PcapngFormatConfig.NAME) : ImmutableList.copyOf(extensions);
    this.stat = stat;
  }

  @JsonProperty("extensions")
  public List<String> getExtensions() {
    return extensions;
  }

  @JsonProperty("stat")
  public boolean getStat() {
    return this.stat;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PcapngFormatConfig that = (PcapngFormatConfig) o;
    return Objects.equals(extensions, that.extensions) && Objects.equals(stat, that.getStat());
  }

  @Override
  public int hashCode() {
    return Objects.hash(extensions, stat);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this).field("extensions", extensions).field("stat", stat).toString();
  }
}
