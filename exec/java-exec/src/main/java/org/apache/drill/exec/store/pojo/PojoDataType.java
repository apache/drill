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
package org.apache.drill.exec.store.pojo;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Timestamp;
import java.util.List;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;

import com.google.common.collect.Lists;

public class PojoDataType {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PojoDataType.class);

  public List<SqlTypeName> types = Lists.newArrayList();
  public List<String> names = Lists.newArrayList();

  public PojoDataType(Class<?> pojoClass) {
    logger.debug(pojoClass.getName());
    Field[] fields = pojoClass.getDeclaredFields();
    for (int i = 0; i < fields.length; i++) {
      Field f = fields[i];

      if (Modifier.isStatic(f.getModifiers())) {
        continue;
      }

      Class<?> type = f.getType();
      names.add(f.getName());

      if (type == int.class || type == Integer.class) {
        types.add(SqlTypeName.INTEGER);
      } else if(type == boolean.class || type == Boolean.class) {
        types.add(SqlTypeName.BOOLEAN);
      } else if(type == long.class || type == Long.class) {
        types.add(SqlTypeName.BIGINT);
      } else if(type == double.class || type == Double.class) {
        types.add(SqlTypeName.DOUBLE);
      } else if(type == String.class) {
        types.add(SqlTypeName.VARCHAR);
      } else if(type.isEnum()) {
        types.add(SqlTypeName.VARCHAR);
      } else if (type == Timestamp.class) {
        types.add(SqlTypeName.TIMESTAMP);
      } else {
        throw new RuntimeException(String.format("PojoRecord reader doesn't yet support conversions from type [%s].", type));
      }
    }
  }

  public RelDataType getRowType(RelDataTypeFactory f) {
    List<RelDataType> fields = Lists.newArrayList();
    for (SqlTypeName n : types) {
      fields.add(f.createSqlType(n));
    }
    return f.createStructType(fields, names);
  }

}
