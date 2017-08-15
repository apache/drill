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


import java.sql.Date;

/**
 * This class is an inheritor of {@link java.sql.Date}, which allows to print five-digit year dates correctly. <p>
 * TODO: There will no need in using of this class, when Drill SQLLine will contain the fix for
 * <a href="https://github.com/julianhyde/sqlline/issues/66">SQLLine date, time, timestamp formats</a> issue is resolved.
 */
public class DatePrintYear extends Date {

  public static final String FIVE_DIGIT_YEAR_FORMAT_DATE = "yyyyy-MM-dd";

  public DatePrintYear(long date) {
    super(date);
  }

  /**
   * Formats a date in the date escape format {@value DatePrintYear#FIVE_DIGIT_YEAR_FORMAT_DATE}.
   * <P>
   * @return a String in {@value DatePrintYear#FIVE_DIGIT_YEAR_FORMAT_DATE} format
   */
  public String toString() {
    int year = super.getYear() + 1900;
    if (year < 10000) {
      return super.toString();
    }
    int month = super.getMonth() + 1;
    int day = super.getDate();

    char buf[] = FIVE_DIGIT_YEAR_FORMAT_DATE.toCharArray();
    buf[0] = Character.forDigit(year / 10000, 10);
    buf[1] = Character.forDigit((year / 1000) % 10, 10);
    buf[2] = Character.forDigit((year / 100) % 10, 10);
    buf[3] = Character.forDigit((year / 10) % 10, 10);
    buf[4] = Character.forDigit(year % 10, 10);
    buf[6] = Character.forDigit(month / 10, 10);
    buf[7] = Character.forDigit(month % 10, 10);
    buf[9] = Character.forDigit(day / 10, 10);
    buf[10] = Character.forDigit(day % 10, 10);

    return new String(buf);
  }
}

