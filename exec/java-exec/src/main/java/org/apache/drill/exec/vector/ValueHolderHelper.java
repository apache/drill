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

import io.netty.buffer.UnpooledByteBufAllocator;

import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.IntervalDayHolder;

import com.google.common.base.Charsets;


public class ValueHolderHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueHolderHelper.class);
  
  public static VarCharHolder getVarCharHolder(String s){
    VarCharHolder vch = new VarCharHolder();
    
    byte[] b = s.getBytes(Charsets.UTF_8);
    vch.start = 0;
    vch.end = b.length;
    vch.buffer = UnpooledByteBufAllocator.DEFAULT.buffer(s.length()); // use the length of input string to allocate buffer. 
    vch.buffer.setBytes(0, b);
    return vch;
  }

  public static IntervalDayHolder getIntervalDayHolder(int days, int millis) {
      IntervalDayHolder dch = new IntervalDayHolder();

      dch.days = days;
      dch.milliSeconds = millis;

      return dch;
  }
}
