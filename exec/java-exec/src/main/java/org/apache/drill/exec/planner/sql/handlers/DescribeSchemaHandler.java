/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.sql.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.CharacterEscapes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlDescribeSchema;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FileSystemSchemaFactory;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;

import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;

public class DescribeSchemaHandler extends DefaultSqlHandler {

  public DescribeSchemaHandler(SqlHandlerConfig config) {
    super(config);
  }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DescribeSchemaHandler.class);
  private static final ObjectMapper mapper = new ObjectMapper(new ObjectMapper().getFactory().setCharacterEscapes(new CharacterEscapes() {
    @Override
    public int[] getEscapeCodesForAscii() {
      // add standard set of escaping characters
      int[] esc = CharacterEscapes.standardAsciiEscapesForJSON();
      // don't escape backslash (not to corrupt windows path)
      esc['\\'] = CharacterEscapes.ESCAPE_NONE;
      return esc;
    }

    @Override
    public SerializableString getEscapeSequence(int i) {
      // no further escaping (beyond ASCII chars) needed
      return null;
    }
  })).enable(INDENT_OUTPUT);


  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) {
    SqlIdentifier schema = ((SqlDescribeSchema) sqlNode).getSchema();
    SchemaPlus drillSchema = SchemaUtilites.findSchema(config.getConverter().getDefaultSchema(), schema.names);

    if (drillSchema != null) {
      StoragePlugin storagePlugin;
      try {
        storagePlugin = context.getStorage().getPlugin(schema.names.get(0));
      } catch (ExecutionSetupException e) {
        throw new DrillRuntimeException("Failure while retrieving storage plugin", e);
      }
      String properties;
      try {
        final Map configMap = mapper.convertValue(storagePlugin.getConfig(), Map.class);
        if (storagePlugin instanceof FileSystemPlugin) {
          transformWorkspaces(schema.names, configMap);
        }
        properties = mapper.writeValueAsString(configMap);
      } catch (JsonProcessingException e) {
        throw new DrillRuntimeException("Error while trying to convert storage config to json string", e);
      }
      return DirectPlan.createDirectPlan(context, new DescribeSchemaResult(Joiner.on(".").join(schema.names), properties));
    }

    throw UserException.validationError()
          .message(String.format("Invalid schema name [%s]", Joiner.on(".").join(schema.names)))
          .build(logger);
  }

  /**
   * If storage plugin has several workspaces, picks appropriate one and removes the others.
   */
  private void transformWorkspaces(List<String> names, Map configMap) {
    Object workspaces = configMap.remove("workspaces");
    if (workspaces != null) {
      Map map = (Map) workspaces;
      String key = names.size() > 1 ? names.get(1) : FileSystemSchemaFactory.DEFAULT_WS_NAME;
      Object workspace = map.get(key);
      if (workspace != null) {
        Map workspaceMap = (Map) map.get(key);
        configMap.putAll(workspaceMap);
      } else if (FileSystemSchemaFactory.DEFAULT_WS_NAME.equals(key)) {
        configMap.putAll(mapper.convertValue(WorkspaceConfig.DEFAULT, Map.class));
      }
    }
  }

  public static class DescribeSchemaResult {
    public String schema;
    public String properties;

    public DescribeSchemaResult(String schema, String properties) {
      this.schema = schema;
      this.properties = properties;
    }
  }

}
