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

package org.apache.drill.exec.store.image;

import java.util.List;

import org.apache.drill.common.logical.FormatPluginConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

@JsonTypeName("image") @JsonInclude(Include.NON_DEFAULT)
public class ImageFormatConfig implements FormatPluginConfig {

  public List<String> extensions = ImmutableList.of();
  public boolean fileSystemMetadata = true;
  public boolean descriptive = true;
  public String timeZone = null;

  public List<String> getExtensions() {
    return extensions;
  }

  public boolean hasFileSystemMetadata() {
    return fileSystemMetadata;
  }

  public boolean isDescriptive() {
    return descriptive;
  }

  public String getTimeZone() {
    return timeZone;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((extensions == null) ? 0 : extensions.hashCode());
    result = prime * result + (fileSystemMetadata ? 1231 : 1237);
    result = prime * result + (descriptive ? 1231 : 1237);
    result = prime * result + ((timeZone == null) ? 0 : timeZone.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null) {
      return false;
    } else if (getClass() != obj.getClass()) {
      return false;
    }
    ImageFormatConfig other = (ImageFormatConfig) obj;
    if (extensions == null) {
      if (other.extensions != null) {
        return false;
      }
    } else if (!extensions.equals(other.extensions)) {
      return false;
    }
    if (fileSystemMetadata != other.fileSystemMetadata) {
      return false;
    }
    if (descriptive != other.descriptive) {
      return false;
    }
    if (timeZone == null) {
      if (other.timeZone != null) {
        return false;
      }
    } else if (!timeZone.equals(other.timeZone)) {
      return false;
    }
    return true;
  }
}