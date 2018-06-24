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
package org.apache.drill.common.config;

import java.util.Set;

import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfigBase;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.common.logical.data.LogicalOperatorBase;
import org.apache.drill.common.scanner.persistence.ScanResult;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;


public class LogicalPlanPersistence {
  private final ObjectMapper mapper;

  public LogicalPlanPersistence(DrillConfig conf, ScanResult scanResult) {
    this(conf, scanResult, new ObjectMapper());
  }

  public LogicalPlanPersistence(DrillConfig conf, ScanResult scanResult, ObjectMapper mapper) {
    this.mapper = mapper;

    SimpleModule deserModule = new SimpleModule("LogicalExpressionDeserializationModule")
        .addDeserializer(LogicalExpression.class, new LogicalExpression.De(conf))
        .addDeserializer(SchemaPath.class, new SchemaPath.De());

    mapper.registerModule(deserModule);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
    mapper.configure(Feature.ALLOW_COMMENTS, true);
    mapper.setFilterProvider(new SimpleFilterProvider().setFailOnUnknownId(false));
    registerSubtypes(LogicalOperatorBase.getSubTypes(scanResult));
    registerSubtypes(StoragePluginConfigBase.getSubTypes(scanResult));
    registerSubtypes(FormatPluginConfigBase.getSubTypes(scanResult));
  }

  public ObjectMapper getMapper() {
    return mapper;
  }

  private <T> void registerSubtypes(Set<Class<? extends T>> types) {
    for (Class<? extends T> type : types) {
      mapper.registerSubtypes(type);
    }
  }
}
