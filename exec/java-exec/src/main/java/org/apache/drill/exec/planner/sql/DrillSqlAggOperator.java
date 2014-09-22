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
package org.apache.drill.exec.planner.sql;

import java.util.List;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;

import com.google.common.collect.ImmutableList;

public class DrillSqlAggOperator extends SqlAggFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlAggOperator.class);


  public DrillSqlAggOperator(String name, int argCount) {
    super(name, new SqlIdentifier(name, SqlParserPos.ZERO), SqlKind.OTHER_FUNCTION, DynamicReturnType.INSTANCE, null, new Checker(argCount), SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    return getAny(validator.getTypeFactory());
  }

  private RelDataType getAny(RelDataTypeFactory factory){
    return factory.createSqlType(SqlTypeName.ANY);
//    return new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory);
  }

  @Override
  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(typeFactory.createSqlType(SqlTypeName.ANY));
  }

  @Override
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return getAny(typeFactory);
  }
}
