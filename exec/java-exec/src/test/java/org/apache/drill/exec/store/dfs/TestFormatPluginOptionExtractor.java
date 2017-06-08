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
package org.apache.drill.exec.store.dfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collection;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.RunTimeScan;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonTypeName;


public class TestFormatPluginOptionExtractor {

  @Test
  public void test() {
    DrillConfig config = DrillConfig.create();
    ScanResult scanResult = RunTimeScan.fromPrescan(config);
    FormatPluginOptionExtractor e = new FormatPluginOptionExtractor(scanResult);
    Collection<FormatPluginOptionsDescriptor> options = e.getOptions();
    for (FormatPluginOptionsDescriptor d : options) {
      assertEquals(d.pluginConfigClass.getAnnotation(JsonTypeName.class).value(), d.typeName);
      switch (d.typeName) {
        case "text":
          assertEquals(TextFormatConfig.class, d.pluginConfigClass);
          assertEquals(
              "(type: String, lineDelimiter: String, fieldDelimiter: String, quote: String, escape: String, comment: String, skipFirstLine: boolean, extractHeader: boolean)",
              d.presentParams()
          );
          break;
        case "named":
          assertEquals(NamedFormatPluginConfig.class, d.pluginConfigClass);
          assertEquals("(type: String, name: String)", d.presentParams());
          break;
        case "parquet":
          assertEquals(d.typeName, "(type: String, autoCorrectCorruptDates: boolean)", d.presentParams());
          break;
        case "json":
        case "sequencefile":
        case "avro":
          assertEquals(d.typeName, "(type: String)", d.presentParams());
          break;
        case "httpd":
          assertEquals("(type: String, logFormat: String, timestampFormat: String)", d.presentParams());
          break;
        default:
          fail("add validation for format plugin type " + d.typeName);
      }
    }
  }
}
