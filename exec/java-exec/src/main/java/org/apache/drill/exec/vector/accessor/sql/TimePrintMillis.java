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
package org.apache.drill.exec.vector.accessor.sql;

import java.sql.Time;

import org.apache.drill.exec.expr.fn.impl.DateUtility;

/**
 * This class is an inheritor of {@link java.sql.Time}, which allows to print time milliseconds correctly. <p>
 * TODO: There will no need in using of this class, when Drill SQLLine will contain the fix for
 * <a href="https://github.com/julianhyde/sqlline/issues/66">SQLLine date, time, timestamp formats</a> issue is resolved.
 */
public class TimePrintMillis extends Time {
  private static final String[] leadingZeroes = {"", "0", "00"};

  // Desired length of the milli second portion should be 3
  private static final int DESIRED_MILLIS_LENGTH = 3;

  public TimePrintMillis(long time) {
    super(time);
  }

  @Override
  public String toString () {
    int millis = (int) (getTime() % DateUtility.secondsToMillis);
    StringBuilder time = new StringBuilder().append(super.toString());

    if (millis > 0) {
      String millisString = Integer.toString(millis);

      // dot to separate the fractional seconds
      time.append(".");

      int millisLength = millisString.length();
      if (millisLength < DESIRED_MILLIS_LENGTH) {
        // add necessary leading zeroes
        time.append(leadingZeroes[DESIRED_MILLIS_LENGTH - millisLength]);
      }
      time.append(millisString);
    }

    return time.toString();
  }
}

