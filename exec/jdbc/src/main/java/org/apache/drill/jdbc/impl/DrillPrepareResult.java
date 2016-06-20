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
package org.apache.drill.jdbc.impl;

import org.apache.drill.exec.proto.UserProtos.PreparedStatement;

import java.util.Collections;
import java.util.List;

import net.hydromatic.avatica.AvaticaParameter;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.ColumnMetaData;

class DrillPrepareResult implements AvaticaPrepareResult{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillPrepareResult.class);

  final String sql;
  final DrillColumnMetaDataList columns = new DrillColumnMetaDataList();
  final PreparedStatement preparedStatement;

  DrillPrepareResult(String sql) {
    this.sql = sql;
    this.preparedStatement = null;
  }

  DrillPrepareResult(String sql, PreparedStatement preparedStatement) {
    this.sql = sql;
    this.preparedStatement = preparedStatement;
    columns.updateColumnMetaData(preparedStatement.getColumnsList());
  }

  @Override
  public List<ColumnMetaData> getColumnList() {
    return columns;
  }

  @Override
  public String getSql() {
    return sql;
  }


  public PreparedStatement getPreparedStatement() {
    return preparedStatement;
  }

  @Override
  public List<AvaticaParameter> getParameterList() {
    return Collections.emptyList();
  }
}
