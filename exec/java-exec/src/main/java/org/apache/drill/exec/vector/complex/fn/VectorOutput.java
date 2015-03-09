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
package org.apache.drill.exec.vector.complex.fn;

import java.io.IOException;

import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Decimal38DenseHolder;
import org.apache.drill.exec.expr.holders.IntervalHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.DateWriter;
import org.apache.drill.exec.vector.complex.writer.IntervalWriter;
import org.apache.drill.exec.vector.complex.writer.TimeStampWriter;
import org.apache.drill.exec.vector.complex.writer.TimeWriter;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.ISOPeriodFormat;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

abstract class VectorOutput {

  final VarBinaryHolder binary = new VarBinaryHolder();
  final TimeHolder time = new TimeHolder();
  final DateHolder date = new DateHolder();
  final TimeStampHolder timestamp = new TimeStampHolder();
  final IntervalHolder interval = new IntervalHolder();
  final BigIntHolder bigint = new BigIntHolder();
  final Decimal38DenseHolder decimal = new Decimal38DenseHolder();
  final VarCharHolder varchar = new VarCharHolder();

  protected final WorkingBuffer work;
  protected JsonParser parser;


  public VectorOutput(WorkingBuffer work){
    this.work = work;
  }

  public void setParser(JsonParser parser){
    this.parser = parser;
  }

  protected boolean innerRun() throws IOException{
    JsonToken t = parser.nextToken();
    if(t != JsonToken.FIELD_NAME){
      return false;
    }

    String possibleTypeName = parser.getText();
    if(!possibleTypeName.isEmpty() && possibleTypeName.charAt(0) == '$'){
      switch(possibleTypeName){
      case ExtendedTypeName.BINARY:
        writeBinary(checkNextToken(JsonToken.VALUE_STRING));
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.DATE:
        writeDate(checkNextToken(JsonToken.VALUE_STRING));
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.TIME:
        writeTime(checkNextToken(JsonToken.VALUE_STRING));
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.TIMESTAMP:
        writeTimestamp(checkNextToken(JsonToken.VALUE_STRING));
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.INTERVAL:
        writeInterval(checkNextToken(JsonToken.VALUE_STRING));
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.INTEGER:
        writeInteger(checkNextToken(JsonToken.VALUE_NUMBER_INT));
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      case ExtendedTypeName.DECIMAL:
        writeDecimal(checkNextToken(JsonToken.VALUE_NUMBER_FLOAT, JsonToken.VALUE_NUMBER_INT));
        checkNextToken(JsonToken.END_OBJECT);
        return true;
      }
    }
    return false;
  }

  public boolean checkNextToken(final JsonToken expected) throws IOException{
    return checkNextToken(expected, expected);
  }

  public boolean checkNextToken(final JsonToken expected1, final JsonToken expected2) throws IOException{
    JsonToken t = parser.nextToken();
    if(t == JsonToken.VALUE_NULL){
      return true;
    }else if(t == expected1){
      return false;
    }else if(t == expected2){
      return false;
    }else{
      throw new JsonParseException(String.format("Failure while reading ExtendedJSON typed value. Expected a %s but "
          + "received a token of type %s", expected1, t), parser.getCurrentLocation());
    }
  }

  public abstract void writeBinary(boolean isNull) throws IOException;
  public abstract void writeDate(boolean isNull) throws IOException;
  public abstract void writeTime(boolean isNull) throws IOException;
  public abstract void writeTimestamp(boolean isNull) throws IOException;
  public abstract void writeInterval(boolean isNull) throws IOException;
  public abstract void writeInteger(boolean isNull) throws IOException;
  public abstract void writeDecimal(boolean isNull) throws IOException;

  static class ListVectorOutput extends VectorOutput{
    private ListWriter writer;

    public ListVectorOutput(WorkingBuffer work) {
      super(work);
    }

    public boolean run(ListWriter writer) throws IOException{
      this.writer = writer;
      return innerRun();
    }

    @Override
    public void writeBinary(boolean isNull) throws IOException {
      VarBinaryWriter bin = writer.varBinary();
      if(!isNull){
        work.prepareBinary(parser.getBinaryValue(), binary);
        bin.write(binary);
      }
    }

    @Override
    public void writeDate(boolean isNull) throws IOException {
      DateWriter dt = writer.date();
      if(!isNull){
        work.prepareVarCharHolder(parser.getValueAsString(), varchar);
        dt.writeDate(StringFunctionHelpers.getDate(varchar.buffer, varchar.start, varchar.end));
      }
    }

    @Override
    public void writeTime(boolean isNull) throws IOException {
      TimeWriter t = writer.time();
      if(!isNull){
        DateTimeFormatter f = ISODateTimeFormat.time();
        t.writeTime((int) ((f.parseDateTime(parser.getValueAsString())).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis()));
      }
    }

    @Override
    public void writeTimestamp(boolean isNull) throws IOException {
      TimeStampWriter ts = writer.timeStamp();
      if(!isNull){
        DateTimeFormatter f = ISODateTimeFormat.dateTime();
        ts.writeTimeStamp(DateTime.parse(parser.getValueAsString(), f).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis());
      }
    }

    @Override
    public void writeInterval(boolean isNull) throws IOException {
      IntervalWriter intervalWriter = writer.interval();
      if(!isNull){
        final Period p = ISOPeriodFormat.standard().parsePeriod(parser.getValueAsString());
        int months = DateUtility.monthsFromPeriod(p);
        int days = p.getDays();
        int millis = DateUtility.millisFromPeriod(p);
        intervalWriter.writeInterval(months, days, millis);
      }
    }

    @Override
    public void writeInteger(boolean isNull) throws IOException {
      BigIntWriter intWriter = writer.bigInt();
      if(!isNull){
        intWriter.writeBigInt(parser.getLongValue());
      }
    }

    @Override
    public void writeDecimal(boolean isNull) throws IOException {
      throw new JsonParseException("Decimal Extended types not yet supported.", parser.getCurrentLocation());
    }

  }

  static class MapVectorOutput extends VectorOutput {

    private MapWriter writer;
    private String fieldName;

    public MapVectorOutput(WorkingBuffer work) {
      super(work);
    }

    public boolean run(MapWriter writer, String fieldName) throws IOException{
      this.fieldName = fieldName;
      this.writer = writer;
      return innerRun();
    }

    @Override
    public void writeBinary(boolean isNull) throws IOException {
      VarBinaryWriter bin = writer.varBinary(fieldName);
      if(!isNull){
        work.prepareBinary(parser.getBinaryValue(), binary);
        bin.write(binary);
      }
    }

    @Override
    public void writeDate(boolean isNull) throws IOException {
      DateWriter dt = writer.date(fieldName);
      if(!isNull){
        DateTimeFormatter f = ISODateTimeFormat.date();
        DateTime date = f.parseDateTime(parser.getValueAsString());
        dt.writeDate(date.withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis());
      }
    }

    @Override
    public void writeTime(boolean isNull) throws IOException {
      TimeWriter t = writer.time(fieldName);
      if(!isNull){
        DateTimeFormatter f = ISODateTimeFormat.time();
        t.writeTime((int) ((f.parseDateTime(parser.getValueAsString())).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis()));
      }
    }

    @Override
    public void writeTimestamp(boolean isNull) throws IOException {
      TimeStampWriter ts = writer.timeStamp(fieldName);
      if(!isNull){
        DateTimeFormatter f = ISODateTimeFormat.dateTime();
        ts.writeTimeStamp(DateTime.parse(parser.getValueAsString(), f).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis());
      }
    }

    @Override
    public void writeInterval(boolean isNull) throws IOException {
      IntervalWriter intervalWriter = writer.interval(fieldName);
      if(!isNull){
        final Period p = ISOPeriodFormat.standard().parsePeriod(parser.getValueAsString());
        int months = DateUtility.monthsFromPeriod(p);
        int days = p.getDays();
        int millis = DateUtility.millisFromPeriod(p);
        intervalWriter.writeInterval(months, days, millis);
      }
    }

    @Override
    public void writeInteger(boolean isNull) throws IOException {
      BigIntWriter intWriter = writer.bigInt(fieldName);
      if(!isNull){
        intWriter.writeBigInt(parser.getLongValue());
      }
    }

    @Override
    public void writeDecimal(boolean isNull) throws IOException {
      throw new IOException("Decimal Extended types not yet supported.");
    }

  }

}
