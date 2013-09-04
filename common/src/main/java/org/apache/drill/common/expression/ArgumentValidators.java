/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.common.expression;

import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;

import com.google.common.base.Predicate;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

public class ArgumentValidators {
  
  private static final String[] EMPTY_NAMES = new String[0];

  public static class AnyTypeAllowed implements ArgumentValidator {

    private final Range<Integer> argumentCount;

    public AnyTypeAllowed(int argCount) {
      super();
      argumentCount = Range.singleton(argCount);
    }

    public AnyTypeAllowed(int minArguments, int maxArguments) {
      super();
      argumentCount = Range.closedOpen(minArguments, maxArguments);
    }

    @Override
    public void validateArguments(ExpressionPosition expr, List<LogicalExpression> expressions, ErrorCollector errors) {
      // only need to check argument count since any type is allowed.
      if (!argumentCount.contains(expressions.size()))
        errors.addUnexpectedArgumentCount(expr, expressions.size(), argumentCount);
    }

    @Override
    public String[] getArgumentNamesByPosition() {
      return EMPTY_NAMES;
    }

  }

  private static class PredicateValidator implements ArgumentValidator {
    private final Range<Integer> argumentCount;
    private Predicate<MajorType> predicate;
    private boolean allSame;

    public PredicateValidator(int argCount, Predicate<MajorType> predicate, boolean allSame) {
      super();
      this.argumentCount = Range.singleton(argCount);
      this.predicate = predicate;
      this.allSame = allSame;
    }

    public PredicateValidator(int minArguments, int maxArguments, Predicate<MajorType> predicate, boolean allSame) {
      super();
      this.argumentCount = Range.closedOpen(minArguments, maxArguments);
      this.predicate = predicate;
      this.allSame = allSame;
    }

    @Override
    public void validateArguments(ExpressionPosition expr, List<LogicalExpression> expressions, ErrorCollector errors) {
      int i = -1;
      MajorType t = null;
      for (LogicalExpression le : expressions) {
        i++;
          MajorType majorType = le.getMajorType();
          if (t == null) t = majorType;

        if (!predicate.apply(majorType)) {
          errors.addUnexpectedType(expr, i, majorType);
          continue;
        }

        if (allSame && !Types.isLateBind(t) && !Types.isLateBind(majorType) && !Types.softEquals(t, majorType, true)) {
          errors.addUnexpectedType(expr, i, majorType);
        }

      }
      if (!argumentCount.contains(expressions.size()))
        errors.addUnexpectedArgumentCount(expr, expressions.size(), argumentCount);
    }

    @Override
    public String[] getArgumentNamesByPosition() {
      return EMPTY_NAMES;
    }
  }

  public static class ComparableArguments extends PredicateValidator {

    public ComparableArguments(int argCount, MajorType... allowedTypes) {
      super(argCount, new ComparableChecker(), true);
    }

    public ComparableArguments(int minArguments, int maxArguments, MajorType... allowedTypes) {
      super(minArguments, maxArguments, new ComparableChecker(), true);
    }

    public static class ComparableChecker implements Predicate<MajorType> {

      public boolean apply(MajorType dt) {
          Types.Comparability comparability = Types.getComparability(dt);
          return comparability.equals(Types.Comparability.ORDERED) || comparability.equals(Types.Comparability.UNKNOWN);
      }
    }
  }

  public static class AllowedTypeList extends PredicateValidator {

    public AllowedTypeList(int argCount, MajorType... allowedTypes) {
      super(argCount, new AllowedTypeChecker(allowedTypes), false);
    }

    public AllowedTypeList(int minArguments, int maxArguments, MajorType... allowedTypes) {
      super(minArguments, maxArguments, new AllowedTypeChecker(allowedTypes), false);
    }

    public AllowedTypeList(int argCount, boolean allSame, MajorType... allowedTypes) {
      super(argCount, new AllowedTypeChecker(allowedTypes), allSame);
    }

    public AllowedTypeList(int minArguments, int maxArguments, boolean allSame, MajorType... allowedTypes) {
      super(minArguments, maxArguments, new AllowedTypeChecker(allowedTypes), allSame);
    }

    public static class AllowedTypeChecker implements Predicate<MajorType> {

      private MajorType[] allowedTypes;

      public AllowedTypeChecker(MajorType... allowedTypes) {
        this.allowedTypes = allowedTypes;
      }

      public boolean apply(MajorType dt) {
        return ArrayUtils.contains(allowedTypes, dt);
      }
    }

  }

  public static class NumericTypeAllowed extends PredicateValidator {

    public NumericTypeAllowed(int argCount, boolean allSame) {
      super(argCount, new NumericTypeChecker(), allSame);

    }

    public NumericTypeAllowed(int minArguments, int maxArguments, boolean allSame) {
      super(minArguments, maxArguments, new NumericTypeChecker(), allSame);
    }

    public static class NumericTypeChecker implements Predicate<MajorType> {

      public boolean apply(MajorType dt) {
        return Types.isNumericType(dt);
      }
    }

  }
}
