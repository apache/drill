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
package org.apache.drill.exec.vector;

import io.netty.buffer.SwappedByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.math.BigDecimal;
import java.nio.ByteOrder;

import org.apache.drill.common.util.DecimalUtility;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.IntervalDayHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import com.google.common.base.Charsets;


public class ValueHolderHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueHolderHelper.class);

  public static VarCharHolder getVarCharHolder(String s){
    VarCharHolder vch = new VarCharHolder();

    byte[] b = s.getBytes(Charsets.UTF_8);
    vch.start = 0;
    vch.end = b.length;
    vch.buffer = UnpooledByteBufAllocator.DEFAULT.buffer(b.length).order(ByteOrder.LITTLE_ENDIAN); // use the length of input string to allocate buffer. 
    vch.buffer.setBytes(0, b);
    return vch;
  }

  public static IntervalDayHolder getIntervalDayHolder(int days, int millis) {
      IntervalDayHolder dch = new IntervalDayHolder();

      dch.days = days;
      dch.milliSeconds = millis;
      return dch;
  }

  public static Decimal9Holder getDecimal9Holder(int decimal, int scale, int precision) {
    Decimal9Holder dch = new Decimal9Holder();

    dch.scale = scale;
    dch.precision = precision;
    dch.value = decimal;

    return dch;
  }

  public static Decimal18Holder getDecimal18Holder(long decimal, int scale, int precision) {
    Decimal18Holder dch = new Decimal18Holder();

    dch.scale = scale;
    dch.precision = precision;
    dch.value = decimal;

    return dch;
  }

  public static Decimal28SparseHolder getDecimal28Holder(String decimal) {

    Decimal28SparseHolder dch = new Decimal28SparseHolder();

    BigDecimal bigDecimal = new BigDecimal(decimal);

    dch.scale = bigDecimal.scale();
    dch.precision = bigDecimal.precision();
    dch.sign = (bigDecimal.signum() == -1);
    dch.start = 0;

    dch.buffer = Unpooled.wrappedBuffer(new byte[5 * DecimalUtility.integerSize]);
    dch.buffer = new SwappedByteBuf(dch.buffer);
    DecimalUtility.getSparseFromBigDecimal(bigDecimal, dch.buffer, dch.start, dch.scale, dch.precision, dch.nDecimalDigits);

    return dch;
  }

  public static Decimal38SparseHolder getDecimal38Holder(String decimal) {


      Decimal38SparseHolder dch = new Decimal38SparseHolder();

      BigDecimal bigDecimal = new BigDecimal(decimal);

      dch.scale = bigDecimal.scale();
      dch.precision = bigDecimal.precision();
      dch.sign = (bigDecimal.signum() == -1);
      dch.start = 0;


      dch.buffer = Unpooled.wrappedBuffer(new byte[dch.maxPrecision * DecimalUtility.integerSize]);
      dch.buffer = new SwappedByteBuf(dch.buffer);
      DecimalUtility.getSparseFromBigDecimal(bigDecimal, dch.buffer, dch.start, dch.scale, dch.precision, dch.nDecimalDigits);

      return dch;
  }
}
