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
package org.apache.drill.exec.compile.bytecode;

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.DateHolder;
import org.apache.arrow.vector.holders.Decimal18Holder;
import org.apache.arrow.vector.holders.Decimal28DenseHolder;
import org.apache.arrow.vector.holders.Decimal28SparseHolder;
import org.apache.arrow.vector.holders.Decimal38DenseHolder;
import org.apache.arrow.vector.holders.Decimal38SparseHolder;
import org.apache.arrow.vector.holders.Decimal9Holder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.IntervalHolder;
import org.apache.arrow.vector.holders.IntervalYearHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateHolder;
import org.apache.arrow.vector.holders.NullableDecimal18Holder;
import org.apache.arrow.vector.holders.NullableDecimal28DenseHolder;
import org.apache.arrow.vector.holders.NullableDecimal28SparseHolder;
import org.apache.arrow.vector.holders.NullableDecimal38DenseHolder;
import org.apache.arrow.vector.holders.NullableDecimal38SparseHolder;
import org.apache.arrow.vector.holders.NullableDecimal9Holder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalHolder;
import org.apache.arrow.vector.holders.NullableIntervalYearHolder;
import org.apache.arrow.vector.holders.NullableTimeHolder;
import org.apache.arrow.vector.holders.NullableTimeStampHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.TimeHolder;
import org.apache.arrow.vector.holders.TimeStampHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.google.common.collect.ImmutableSet;

/**
 * Reference list of classes we will perform scalar replacement on.
 */
public class ScalarReplacementTypes {
  // This class only has static utilities.
  private ScalarReplacementTypes() {
  }

  static {
    Class<?>[] classList = {
        BitHolder.class,
        IntHolder.class,
        BigIntHolder.class,
        Float4Holder.class,
        Float8Holder.class,
        Decimal9Holder.class,
        Decimal18Holder.class,
        Decimal28SparseHolder.class,
        Decimal28DenseHolder.class,
        Decimal38SparseHolder.class,
        Decimal38DenseHolder.class,
        IntervalHolder.class,
        IntervalDayHolder.class,
        IntervalYearHolder.class,
        DateHolder.class,
        TimeHolder.class,
        TimeStampHolder.class,
        VarCharHolder.class,
        VarBinaryHolder.class,
        NullableBitHolder.class,
        NullableIntHolder.class,
        NullableBigIntHolder.class,
        NullableFloat4Holder.class,
        NullableFloat8Holder.class,
        NullableVarCharHolder.class,
        NullableVarBinaryHolder.class,
        NullableDecimal9Holder.class,
        NullableDecimal18Holder.class,
        NullableDecimal28SparseHolder.class,
        NullableDecimal28DenseHolder.class,
        NullableDecimal38SparseHolder.class,
        NullableDecimal38DenseHolder.class,
        NullableIntervalHolder.class,
        NullableIntervalDayHolder.class,
        NullableIntervalYearHolder.class,
        NullableDateHolder.class,
        NullableTimeHolder.class,
        NullableTimeStampHolder.class,
    };

    CLASSES = ImmutableSet.copyOf(classList);
  }

  public static final ImmutableSet<Class<?>> CLASSES;

  /**
   * Determine if a class is a holder class.
   *
   * @param className the name of the class
   * @return true if the class belongs to the CLASSES set.
   */
  public static boolean isHolder(final String className) {
    try {
      final Class<?> clazz = Class.forName(className);
      return CLASSES.contains(clazz);
    } catch (ClassNotFoundException e) {
      // do nothing
    }

    return false;
  }
}
