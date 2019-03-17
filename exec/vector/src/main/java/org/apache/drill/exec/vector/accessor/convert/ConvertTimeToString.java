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

import org.apache.drill.exec.vector.accessor.InvalidConversionError;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class ConvertTimeToString extends AbstractWriteConverter {

  private final DateTimeFormatter dateTimeFormatter;

  public ConvertTimeToString(ScalarWriter baseWriter) {
    super(baseWriter);
    final String formatValue = baseWriter.schema().format();
    dateTimeFormatter = formatValue == null
      ? ISODateTimeFormat.time() : DateTimeFormat.forPattern(formatValue);
  }

  @Override
  public void setTime(final LocalTime value) {
    if (value == null) {
      baseWriter.setNull();
    } else {
      try {
        baseWriter.setString(dateTimeFormatter.print(value));
      }
      catch (final IllegalStateException e) {
        throw InvalidConversionError.writeError(schema(), value, e);
      }
    }
  }

  @Override
  public void setValue(Object value) {
    if (value == null) {
      setNull();
    } else {
      setTime((LocalTime) value);
    }
  }
}
