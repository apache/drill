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

package org.apache.drill.exec.store.elasticsearch.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.elasticsearch.ElasticSearchScanSpec;

public class DrillElasticsearchTable extends DynamicDrillTable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillElasticsearchTable.class);


  public DrillElasticsearchTable(StoragePlugin plugin, String storageEngineName, String userName, ElasticSearchScanSpec scanSpec) {
    super(plugin, storageEngineName, scanSpec);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {

    return super.getRowType(typeFactory);
  }
  
  
//  @Override
//  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//    ArrayList<RelDataType> typeList = new ArrayList<>();
//    ArrayList<String> fieldNameList = new ArrayList<>();
//    
//    
//    // 拿到相关的字段
//    fieldNameList.add(ROW_KEY);
//    typeList.add(typeFactory.createSqlType(SqlTypeName.ANY));
//
//    Set<byte[]> families = tableDesc.getFamiliesKeys();
//    for (byte[] family : families) {
//      fieldNameList.add(Bytes.toString(family));
//      // 列族 key value
//      typeList.add(typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.ANY)));
//    }
//    return typeFactory.createStructType(typeList, fieldNameList);
//  }
 
}
