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
package org.apache.drill.exec.vector.accessor.convert;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.drill.exec.vector.accessor.InvalidConversionError;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

/**
 * Convert a VARCHAR column to an DATE column following the Java rules
 * for parsing a date time, optionally using the formatter provided in
 * the column schema.
 */
public class ConvertStringToDate extends AbstractWriteConverter {

  private static ZoneId UTC = ZoneId.of("Z");
  private final DateTimeFormatter dateTimeFormatter;

  public ConvertStringToDate(ScalarWriter baseWriter) {
    super(baseWriter);
    dateTimeFormatter = baseWriter.schema().dateTimeFormatter();
  }

  @Override
  public void setString(String value) {
    if (value == null) {
      baseWriter.setNull();
    } else {
      try {
        final LocalDate dt = LocalDate.parse(value, dateTimeFormatter);
        baseWriter.setLong(dt.atStartOfDay(UTC).toInstant().toEpochMilli());
      }
      catch (final DateTimeParseException e) {
        throw InvalidConversionError.writeError(schema(), value, e);
      }
    }
  }
}
