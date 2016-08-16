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

import static java.util.Collections.unmodifiableMap;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.TableInstance;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.TableParamDef;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.TableSignature;
import org.slf4j.Logger;

import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Describes the options for a format plugin
 * extracted from the FormatPluginConfig subclass
 */
final class FormatPluginOptionsDescriptor {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(FormatPluginOptionsDescriptor.class);

  final Class<? extends FormatPluginConfig> pluginConfigClass;
  final String typeName;
  private final Map<String, TableParamDef> functionParamsByName;

  /**
   * Uses reflection to extract options based on the fields of the provided config class
   * ("List extensions" field is ignored, pending removal, Char is turned into String)
   * The class must be annotated with {@code @JsonTypeName("type name")}
   * @param pluginConfigClass the config class we want to extract options from through reflection
   */
  FormatPluginOptionsDescriptor(Class<? extends FormatPluginConfig> pluginConfigClass) {
    this.pluginConfigClass = pluginConfigClass;
    Map<String, TableParamDef> paramsByName = new LinkedHashMap<>();
    Field[] fields = pluginConfigClass.getDeclaredFields();
    // @JsonTypeName("text")
    JsonTypeName annotation = pluginConfigClass.getAnnotation(JsonTypeName.class);
    this.typeName = annotation != null ? annotation.value() : null;
    if (this.typeName != null) {
      paramsByName.put("type", new TableParamDef("type", String.class));
    }
    for (Field field : fields) {
      if (Modifier.isStatic(field.getModifiers())
          // we want to deprecate this field
          || (field.getName().equals("extensions") && field.getType() == List.class)) {
        continue;
      }
      Class<?> fieldType = field.getType();
      if (fieldType == char.class) {
        // calcite does not like char type. Just use String and enforce later that length == 1
        fieldType = String.class;
      }
      paramsByName.put(field.getName(), new TableParamDef(field.getName(), fieldType).optional());
    }
    this.functionParamsByName = unmodifiableMap(paramsByName);
  }

  /**
   * returns the table function signature for this format plugin config class
   * @param tableName the table for which we want a table function signature
   * @return the signature
   */
  TableSignature getTableSignature(String tableName) {
    return new TableSignature(tableName, params());
  }

  /**
   * @return the parameters extracted from the provided format plugin config class
   */
  private List<TableParamDef> params() {
    return new ArrayList<>(functionParamsByName.values());
  }

  /**
   * @return a readable String of the parameters and their names
   */
  String presentParams() {
    StringBuilder sb = new StringBuilder("(");
    List<TableParamDef> params = params();
    for (int i = 0; i < params.size(); i++) {
      TableParamDef paramDef = params.get(i);
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(paramDef.name).append(": ").append(paramDef.type.getSimpleName());
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * creates an instance of the FormatPluginConfig based on the passed parameters
   * @param t the signature and the parameters passed to the table function
   * @return the corresponding config
   */
  FormatPluginConfig createConfigForTable(TableInstance t) {
    // Per the constructor, the first param is always "type"
    TableParamDef typeParamDef = t.sig.params.get(0);
    Object typeParam = t.params.get(0);
    if (!typeParamDef.name.equals("type")
        || typeParamDef.type != String.class
        || !(typeParam instanceof String)
        || !typeName.equalsIgnoreCase((String)typeParam)) {
      // if we reach here, there's a bug as all signatures generated start with a type parameter
      throw UserException.parseError()
          .message(
              "This function signature is not supported: %s\n"
              + "expecting %s",
              t.presentParams(), this.presentParams())
          .addContext("table", t.sig.name)
          .build(logger);
    }
    FormatPluginConfig config;
    try {
      config = pluginConfigClass.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw UserException.parseError(e)
          .message(
              "configuration for format of type %s can not be created (class: %s)",
              this.typeName, pluginConfigClass.getName())
          .addContext("table", t.sig.name)
          .build(logger);
    }
    for (int i = 1; i < t.params.size(); i++) {
      Object param = t.params.get(i);
      if (param == null) {
        // when null is passed, we leave the default defined in the config class
        continue;
      }
      if (param instanceof String) {
        // normalize Java literals, ex: \t, \n, \r
        param = StringEscapeUtils.unescapeJava((String) param);
      }
      TableParamDef paramDef = t.sig.params.get(i);
      TableParamDef expectedParamDef = this.functionParamsByName.get(paramDef.name);
      if (expectedParamDef == null || expectedParamDef.type != paramDef.type) {
        throw UserException.parseError()
        .message(
            "The parameters provided are not applicable to the type specified:\n"
                + "provided: %s\nexpected: %s",
            t.presentParams(), this.presentParams())
        .addContext("table", t.sig.name)
        .build(logger);
      }
      try {
        Field field = pluginConfigClass.getField(paramDef.name);
        field.setAccessible(true);
        if (field.getType() == char.class && param instanceof String) {
          String stringParam = (String) param;
          if (stringParam.length() != 1) {
            throw UserException.parseError()
              .message("Expected single character but was String: %s", stringParam)
              .addContext("table", t.sig.name)
              .addContext("parameter", paramDef.name)
              .build(logger);
          }
          param = stringParam.charAt(0);
        }
        field.set(config, param);
      } catch (IllegalAccessException | NoSuchFieldException | SecurityException e) {
        throw UserException.parseError(e)
            .message("can not set value %s to parameter %s: %s", param, paramDef.name, paramDef.type)
            .addContext("table", t.sig.name)
            .addContext("parameter", paramDef.name)
            .build(logger);
      }
    }
    return config;
  }

  @Override
  public String toString() {
    return "OptionsDescriptor [pluginConfigClass=" + pluginConfigClass + ", typeName=" + typeName
        + ", functionParamsByName=" + functionParamsByName + "]";
  }
}