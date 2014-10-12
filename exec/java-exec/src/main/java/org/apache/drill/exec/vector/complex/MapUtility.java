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
package org.apache.drill.exec.vector.complex;

import com.google.common.base.Charsets;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.fn.impl.MappifyUtility;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.IntervalDayHolder;
import org.apache.drill.exec.expr.holders.IntervalHolder;
import org.apache.drill.exec.expr.holders.IntervalYearHolder;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.TinyIntHolder;
import org.apache.drill.exec.expr.holders.UInt1Holder;
import org.apache.drill.exec.expr.holders.UInt2Holder;
import org.apache.drill.exec.expr.holders.UInt4Holder;
import org.apache.drill.exec.expr.holders.UInt8Holder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.joda.time.Period;

public class MapUtility {
  /*
   * Function to read a value from the field reader, detect the type, construct the appropriate value holder
   * and use the value holder to write to the Map.
   */
  public static void writeToMapFromReader(FieldReader fieldReader, BaseWriter.MapWriter mapWriter, DrillBuf buffer) {

    MajorType valueMajorType = fieldReader.getType();
    MinorType valueMinorType = valueMajorType.getMinorType();

    switch (valueMinorType) {
      case TINYINT:
        TinyIntHolder tinyIntHolder = new TinyIntHolder();
        tinyIntHolder.value = fieldReader.readByte();
        mapWriter.tinyInt(MappifyUtility.fieldValue).write(tinyIntHolder);
        break;
      case SMALLINT:
        SmallIntHolder smallIntHolder = new SmallIntHolder();
        smallIntHolder.value = fieldReader.readShort();
        mapWriter.smallInt(MappifyUtility.fieldValue).write(smallIntHolder);
        break;
      case BIGINT:
        BigIntHolder bh = new BigIntHolder();
        bh.value = fieldReader.readLong();
        mapWriter.bigInt(MappifyUtility.fieldValue).write(bh);
        break;
      case INT:
        IntHolder ih = new IntHolder();
        ih.value = fieldReader.readInteger();
        mapWriter.integer(MappifyUtility.fieldValue).write(ih);
        break;
      case UINT1:
        UInt1Holder uInt1Holder = new UInt1Holder();
        uInt1Holder.value = fieldReader.readByte();
        mapWriter.uInt1(MappifyUtility.fieldValue).write(uInt1Holder);
        break;
      case UINT2:
        UInt2Holder uInt2Holder = new UInt2Holder();
        uInt2Holder.value = fieldReader.readCharacter();
        mapWriter.uInt2(MappifyUtility.fieldValue).write(uInt2Holder);
        break;
      case UINT4:
        UInt4Holder uInt4Holder = new UInt4Holder();
        uInt4Holder.value = fieldReader.readInteger();
        mapWriter.uInt4(MappifyUtility.fieldValue).write(uInt4Holder);
        break;
      case UINT8:
        UInt8Holder uInt8Holder = new UInt8Holder();
        uInt8Holder.value = fieldReader.readInteger();
        mapWriter.uInt8(MappifyUtility.fieldValue).write(uInt8Holder);
        break;
      case DECIMAL9:
        Decimal9Holder decimalHolder = new Decimal9Holder();
        decimalHolder.value = fieldReader.readBigDecimal().intValue();
        decimalHolder.scale = valueMajorType.getScale();
        decimalHolder.precision = valueMajorType.getPrecision();
        mapWriter.decimal9(MappifyUtility.fieldValue).write(decimalHolder);
        break;
      case DECIMAL18:
        Decimal18Holder decimal18Holder = new Decimal18Holder();
        decimal18Holder.value = fieldReader.readBigDecimal().longValue();
        decimal18Holder.scale = valueMajorType.getScale();
        decimal18Holder.precision = valueMajorType.getPrecision();
        mapWriter.decimal18(MappifyUtility.fieldValue).write(decimal18Holder);
        break;
      case DECIMAL28SPARSE:
        Decimal28SparseHolder decimal28Holder = new Decimal28SparseHolder();

        // Ensure that the buffer used to store decimal is of sufficient length
        buffer.reallocIfNeeded(decimal28Holder.WIDTH);
        decimal28Holder.scale = valueMajorType.getScale();
        decimal28Holder.precision = valueMajorType.getPrecision();
        decimal28Holder.buffer = buffer;
        decimal28Holder.start = 0;
        DecimalUtility.getSparseFromBigDecimal(fieldReader.readBigDecimal(), buffer, 0, decimal28Holder.scale,
            decimal28Holder.precision, decimal28Holder.nDecimalDigits);
        mapWriter.decimal28Sparse(MappifyUtility.fieldValue).write(decimal28Holder);
        break;
      case DECIMAL38SPARSE:
        Decimal38SparseHolder decimal38Holder = new Decimal38SparseHolder();

        // Ensure that the buffer used to store decimal is of sufficient length
        buffer.reallocIfNeeded(decimal38Holder.WIDTH);
        decimal38Holder.scale = valueMajorType.getScale();
        decimal38Holder.precision = valueMajorType.getPrecision();
        decimal38Holder.buffer = buffer;
        decimal38Holder.start = 0;
        DecimalUtility.getSparseFromBigDecimal(fieldReader.readBigDecimal(), buffer, 0, decimal38Holder.scale,
            decimal38Holder.precision, decimal38Holder.nDecimalDigits);

        mapWriter.decimal38Sparse(MappifyUtility.fieldValue).write(decimal38Holder);
        break;
      case DATE:
        DateHolder dateHolder = new DateHolder();
        dateHolder.value = fieldReader.readLong();
        mapWriter.date(MappifyUtility.fieldValue).write(dateHolder);
        break;
      case TIME:
        TimeHolder timeHolder = new TimeHolder();
        timeHolder.value = fieldReader.readInteger();
        mapWriter.time(MappifyUtility.fieldValue).write(timeHolder);
        break;
      case TIMESTAMP:
        TimeStampHolder timeStampHolder = new TimeStampHolder();
        timeStampHolder.value = fieldReader.readLong();
        mapWriter.timeStamp(MappifyUtility.fieldValue).write(timeStampHolder);
        break;
      case INTERVAL:
        IntervalHolder intervalHolder = new IntervalHolder();
        Period period = fieldReader.readPeriod();
        intervalHolder.months = (period.getYears() * org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths) + period.getMonths();
        intervalHolder.days = period.getDays();
        intervalHolder.milliseconds = (period.getHours() * org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis) +
            (period.getMinutes() * org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis) +
            (period.getSeconds() * org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis) +
            (period.getMillis());
        mapWriter.interval(MappifyUtility.fieldValue).write(intervalHolder);
        break;
      case INTERVALDAY:
        IntervalDayHolder intervalDayHolder = new IntervalDayHolder();
        Period periodDay = fieldReader.readPeriod();
        intervalDayHolder.days = periodDay.getDays();
        intervalDayHolder.milliseconds = (periodDay.getHours() * org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis) +
            (periodDay.getMinutes() * org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis) +
            (periodDay.getSeconds() * org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis) +
            (periodDay.getMillis());
        mapWriter.intervalDay(MappifyUtility.fieldValue).write(intervalDayHolder);
        break;
      case INTERVALYEAR:
        IntervalYearHolder intervalYearHolder = new IntervalYearHolder();
        Period periodYear = fieldReader.readPeriod();
        intervalYearHolder.value = (periodYear.getYears() * org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths) + periodYear.getMonths();
        mapWriter.intervalYear(MappifyUtility.fieldValue).write(intervalYearHolder);
        break;
      case FLOAT4:
        Float4Holder float4Holder = new Float4Holder();
        float4Holder.value = fieldReader.readFloat();
        mapWriter.float4(MappifyUtility.fieldValue).write(float4Holder);
        break;
      case FLOAT8:
        Float8Holder float8Holder = new Float8Holder();
        float8Holder.value = fieldReader.readDouble();
        mapWriter.float8(MappifyUtility.fieldValue).write(float8Holder);
        break;
      case BIT:
        BitHolder bitHolder = new BitHolder();
        bitHolder.value = (fieldReader.readBoolean() == true) ? 1 : 0;
        mapWriter.bit(MappifyUtility.fieldValue).write(bitHolder);
        break;
      case VARCHAR:
        VarCharHolder vh1 = new VarCharHolder();
        byte[] b = fieldReader.readText().toString().getBytes(Charsets.UTF_8);
        buffer.reallocIfNeeded(b.length);
        buffer.setBytes(0, b);
        vh1.start = 0;
        vh1.end = b.length;
        vh1.buffer = buffer;
        mapWriter.varChar(MappifyUtility.fieldValue).write(vh1);
        break;
      case VARBINARY:
        VarBinaryHolder varBinaryHolder = new VarBinaryHolder();
        byte[] b1 = fieldReader.readByteArray();
        buffer.reallocIfNeeded(b1.length);
        buffer.setBytes(0, b1);
        varBinaryHolder.start = 0;
        varBinaryHolder.end = b1.length;
        varBinaryHolder.buffer = buffer;
        mapWriter.varBinary(MappifyUtility.fieldValue).write(varBinaryHolder);
        break;
      default:
        throw new DrillRuntimeException(String.format("Mappify does not support input of type: %s", valueMinorType));
    }
  }
}
