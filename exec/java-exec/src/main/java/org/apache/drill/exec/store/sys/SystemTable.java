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
package org.apache.drill.exec.store.sys;

import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.pojo.PojoDataType;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

public enum SystemTable {
  OPTION("options", OptionValue.class),
  DRILLBITS("drillbits", DrillbitIterator.DrillbitInstance.class),
  VERSION("version", VersionIterator.VersionInfo.class)
  ;

  private final PojoDataType type;
  private final String tableName;
  private final Class<?> pojoClass;

  SystemTable(String tableName, Class<?> clazz){
    this.type = new PojoDataType(clazz);
    this.tableName = tableName;
    this.pojoClass = clazz;
  }

  public String getTableName(){
    return tableName;
  }

  public RelDataType getRowType(RelDataTypeFactory f){
    return type.getRowType(f);
  }

  public PojoDataType getType(){
    return type;
  }

  public Class<?> getPojoClass(){
    return pojoClass;
  }
}