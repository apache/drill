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

import java.util.Collection;
import java.util.List;

import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelProtoDataType;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlCallBinding;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperandCountRange;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.type.ExplicitReturnTypeInference;
import org.eigenbase.sql.type.SqlOperandCountRanges;
import org.eigenbase.sql.type.SqlOperandTypeChecker;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;

import com.google.hive12.common.collect.Lists;

public class DrillSqlOperator extends SqlFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlOperator.class);

  DrillSqlOperator(String name, int argCount) {
    super(name, SqlKind.OTHER_FUNCTION, DYNAMIC_RETURN, null, new Checker(argCount), SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  private static final DynamicReturnType DYNAMIC_RETURN = new DynamicReturnType();


  @Override
  public SqlIdentifier getSqlIdentifier() {
    return super.getSqlIdentifier();
  }

  @Override
  public SqlIdentifier getNameAsId() {
    return super.getNameAsId();
  }

  @Override
  public List<RelDataType> getParamTypes() {
    return super.getParamTypes();
  }

  @Override
  public SqlFunctionCategory getFunctionType() {
    return super.getFunctionType();
  }

  @Override
  public boolean isQuantifierAllowed() {
    return super.isQuantifierAllowed();
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    return new RelDataTypeDrillImpl(new RelDataTypeHolder(), validator.getTypeFactory());
//    return validator.getTypeFactory().createSqlType(SqlTypeName.ANY);
    //return super.deriveType(validator, scope, call);
  }

  
  private static class DynamicReturnType extends ExplicitReturnTypeInference {
    public DynamicReturnType() {
      super(new DynamicType());
    }
  }
  private static class DynamicType implements RelProtoDataType {

    @Override
    public RelDataType apply(RelDataTypeFactory factory) {
      return factory.createSqlType(SqlTypeName.ANY);
      //return new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory);
    }

  }

  private static class FixedRange implements SqlOperandCountRange{

    private final int size;
    
    public FixedRange(int size) {
      super();
      this.size = size;
    }

    @Override
    public boolean isValidCount(int count) {
      return count == size;
    }

    @Override
    public int getMin() {
      return size;
    }

    @Override
    public int getMax() {
      return size;
    }
    
  }
  
  private static class Checker implements SqlOperandTypeChecker {
    private SqlOperandCountRange range;

    public Checker(int size) {
      range = new FixedRange(size);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return range;
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + "(Drill - Opaque)";
    }

  }
}
