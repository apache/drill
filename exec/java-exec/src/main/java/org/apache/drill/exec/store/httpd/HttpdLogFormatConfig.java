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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.FormatPluginConfig;

@JsonTypeName("httpd")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class HttpdLogFormatConfig implements FormatPluginConfig {

  public String logFormat;
  public String timestampFormat = "dd/MMM/yyyy:HH:mm:ss ZZ";

  /**
   * @return the log formatting string.  This string is the config string from httpd.conf or similar config file.
   */
  public String getLogFormat() {
    return logFormat;
  }

  public void setLogFormat(String format) {
    this.logFormat = format;
  }

  /**
   * @return the timestampFormat
   */
  public String getTimestampFormat() {
    return timestampFormat;
  }

  /**
   * Sets the time stamp format
   * @param timestamp
   */
  public void setTimestampFormat(String timestamp) {
    this.timestampFormat = timestamp;
  }

  @Override
  public int hashCode() {
    int result = logFormat != null ? logFormat.hashCode() : 0;
    result = 31 * result + (timestampFormat != null ? timestampFormat.hashCode() : 0);
    return result;
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

    if (logFormat != null ? !logFormat.equals(that.logFormat) : that.logFormat != null) {
      return false;
    }
    return timestampFormat != null ? timestampFormat.equals(that.timestampFormat) : that.timestampFormat == null;
  }
}
