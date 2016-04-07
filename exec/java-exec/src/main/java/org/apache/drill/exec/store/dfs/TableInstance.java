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
package org.apache.drill.exec.store.dfs;

import org.apache.drill.common.exceptions.UserException;
import org.apache.hadoop.fs.FileSystem;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * Object representing a table in storage implementing the Hadoop {@link FileSystem} interface.
 */
final class TableInstance {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TableInstance.class);

  final TableSignature sig;
  final List<Object> params;

  TableInstance(TableSignature sig, List<Object> params) {
    super();
    if (params.size() != sig.params.size()) {
      throw UserException.parseError()
          .message(
              "should have as many params (%d) as signature (%d)",
              params.size(), sig.params.size())
          .addContext("table", sig.name)
          .build(logger);
    }
    this.sig = sig;
    this.params = unmodifiableList(params);
  }

  String presentParams() {
    StringBuilder sb = new StringBuilder("(");
    boolean first = true;
    for (int i = 0; i < params.size(); i++) {
      Object param = params.get(i);
      if (param != null) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        TableParamDef paramDef = sig.params.get(i);
        sb.append(paramDef.name).append(": ").append(paramDef.type.getSimpleName()).append(" => ").append(param);
      }
    }
    sb.append(")");
    return sb.toString();
  }

  private Object[] toArray() {
    return array(sig, params);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(toArray());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TableInstance) {
      return Arrays.equals(this.toArray(), ((TableInstance)obj).toArray());
    }
    return false;
  }

  @Override
  public String toString() {
    return sig.name + (params.size() == 0 ? "" : presentParams());
  }

  private static Object[] array(Object... objects) {
    return objects;
  }

  public static final class TableSignature {
    final String name;
    final List<TableParamDef> params;

    TableSignature(String name, TableParamDef... params) {
      this(name, Arrays.asList(params));
    }

    TableSignature(String name, List<TableParamDef> params) {
      this.name = name;
      this.params = unmodifiableList(params);
    }

    private Object[] toArray() {
      return array(name, params);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(toArray());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TableSignature) {
        return Arrays.equals(this.toArray(), ((TableSignature)obj).toArray());
      }
      return false;
    }

    @Override
    public String toString() {
      return name + params;
    }
  }

  public static final class TableParamDef {
    final String name;
    final Class<?> type;
    final boolean optional;

    TableParamDef(String name, Class<?> type) {
      this(name, type, false);
    }

    TableParamDef(String name, Class<?> type, boolean optional) {
      this.name = name;
      this.type = type;
      this.optional = optional;
    }

    TableParamDef optional() {
      return new TableParamDef(name, type, true);
    }

    private Object[] toArray() {
      return array(name, type, optional);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(toArray());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TableParamDef) {
        return Arrays.equals(this.toArray(), ((TableParamDef)obj).toArray());
      }
      return false;
    }

    @Override
    public String toString() {
      String p = name + ": " + type;
      return optional ? "[" + p + "]" : p;
    }
  }
}
