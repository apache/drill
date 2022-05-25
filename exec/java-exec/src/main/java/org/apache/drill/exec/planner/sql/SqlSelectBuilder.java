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
package org.apache.drill.exec.planner.sql;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlSelectBuilder {
  private SqlParserPos parserPosition;
  private SqlNodeList keywordList;
  private SqlNodeList selectList;
  private SqlNode from;
  private SqlNode where;
  private SqlNodeList groupBy;
  private SqlNode having;
  private SqlNodeList windowDecls;
  private SqlNodeList orderBy;
  private SqlNode offset;
  private SqlNode fetch;

  public SqlSelectBuilder parserPosition(SqlParserPos parserPosition) {
    this.parserPosition = parserPosition;
    return this;
  }

  public SqlSelectBuilder keywordList(SqlNodeList keywordList) {
    this.keywordList = keywordList;
    return this;
  }

  public SqlSelectBuilder selectList(SqlNodeList selectList) {
    this.selectList = selectList;
    return this;
  }

  public SqlSelectBuilder from(SqlNode from) {
    this.from = from;
    return this;
  }

  public SqlSelectBuilder where(SqlNode where) {
    this.where = where;
    return this;
  }

  public SqlSelectBuilder groupBy(SqlNodeList groupBy) {
    this.groupBy = groupBy;
    return this;
  }

  public SqlSelectBuilder having(SqlNode having) {
    this.having = having;
    return this;
  }

  public SqlSelectBuilder windowDecls(SqlNodeList windowDecls) {
    this.windowDecls = windowDecls;
    return this;
  }

  public SqlSelectBuilder orderBy(SqlNodeList orderBy) {
    this.orderBy = orderBy;
    return this;
  }

  public SqlSelectBuilder offset(SqlNode offset) {
    this.offset = offset;
    return this;
  }

  public SqlSelectBuilder fetch(SqlNode fetch) {
    this.fetch = fetch;
    return this;
  }

  public SqlSelect build() {
    return new SqlSelect(parserPosition, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch);
  }

}
