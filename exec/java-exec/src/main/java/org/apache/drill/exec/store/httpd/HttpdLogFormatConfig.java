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
package org.apache.drill.exec.store.httpd;

import java.util.Objects;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("httpd")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class HttpdLogFormatConfig implements FormatPluginConfig {

  public static final String DEFAULT_TS_FORMAT = "dd/MMM/yyyy:HH:mm:ss ZZ";

  // No extensions?
  private final String logFormat;
  private final String timestampFormat;

  @JsonCreator
  public HttpdLogFormatConfig(
      @JsonProperty("logFormat") String logFormat,
      @JsonProperty("timestampFormat") String timestampFormat) {
    this.logFormat = logFormat;
    this.timestampFormat = timestampFormat == null
        ? DEFAULT_TS_FORMAT : timestampFormat;
  }

  /**
   * @return the log formatting string. This string is the config string from
   *         httpd.conf or similar config file.
   */
  public String getLogFormat() {
    return logFormat;
  }

  /**
   * @return the timestampFormat
   */
  public String getTimestampFormat() {
    return timestampFormat;
  }

  @Override
  public int hashCode() {
    return Objects.hash(logFormat, timestampFormat);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HttpdLogFormatConfig that = (HttpdLogFormatConfig) o;
    return Objects.equals(logFormat, that.logFormat) &&
           Objects.equals(timestampFormat, that.timestampFormat);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("log format", logFormat)
        .field("timestamp format", timestampFormat)
        .toString();
  }
}
