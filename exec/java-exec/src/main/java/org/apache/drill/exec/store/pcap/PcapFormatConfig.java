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
package org.apache.drill.exec.store.pcap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

@JsonTypeName(PcapFormatPlugin.PLUGIN_NAME)
public class PcapFormatConfig implements FormatPluginConfig {

  private static final List<String> DEFAULT_EXTS = ImmutableList.of(PcapFormatPlugin.PLUGIN_NAME);

  public List<String> extensions;

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean sessionizeTCPStreams = false;

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    return extensions == null ? DEFAULT_EXTS : extensions;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[]{extensions, sessionizeTCPStreams});
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PcapFormatConfig other = (PcapFormatConfig) obj;
    return Objects.equal(extensions, other.extensions)
      && Objects.equal(sessionizeTCPStreams, other.sessionizeTCPStreams);
  }
}
