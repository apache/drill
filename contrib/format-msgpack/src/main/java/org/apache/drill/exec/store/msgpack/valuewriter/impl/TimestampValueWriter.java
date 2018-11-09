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
package org.apache.drill.exec.store.msgpack.valuewriter.impl;

import java.nio.ByteBuffer;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.store.msgpack.valuewriter.ExtensionValueWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

public class TimestampValueWriter extends AbstractScalarValueWriter implements ExtensionValueWriter {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TimestampValueWriter.class);

  private final ByteBuffer timestampReadBuffer = ByteBuffer.allocate(12);

  public TimestampValueWriter() {
  }

  @Override
  public ValueType getMsgpackValueType() {
    return ValueType.EXTENSION;
  }

  @Override
  public byte getExtensionTypeNumber() {
    return 0;
  }

  @Override
  public MinorType getDefaultType(Value v) {
    return MinorType.TIMESTAMP;
  }

  /**
   * <code>
   * <pre>
   * timestamp 32 stores the number of seconds that have elapsed since 1970-01-01 00:00:00 UTC
   * in an 32-bit unsigned integer:
   * +--------+--------+--------+--------+--------+--------+
   * |  0xd6  |   -1   |   seconds in 32-bit unsigned int  |
   * +--------+--------+--------+--------+--------+--------+
   *
   * timestamp 64 stores the number of seconds and nanoseconds that have elapsed since 1970-01-01 00:00:00 UTC
   * in 32-bit unsigned integers:
   * +--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
   * |  0xd7  |   -1   |nanoseconds in 30-bit unsigned int |  seconds in 34-bit unsigned int   |
   * +--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
   *
   * timestamp 96 stores the number of seconds and nanoseconds that have elapsed since 1970-01-01 00:00:00 UTC
   * in 64-bit signed integer and 32-bit unsigned integer:
   * +--------+--------+--------+--------+--------+--------+--------+
   * |  0xc7  |   12   |   -1   |nanoseconds in 32-bit unsigned int |
   * +--------+--------+--------+--------+--------+--------+--------+
   * +--------+--------+--------+--------+--------+--------+--------+--------+
   *                     seconds in 64-bit signed int                        |
   * +--------+--------+--------+--------+--------+--------+--------+--------+
   *</pre>
   *</code>
   */
  @Override
  public void doWrite(Value v, MapWriter mapWriter, String fieldName, ListWriter listWriter,
      MinorType targetSchemaType) {

    ExtensionValue value = v.asExtensionValue();
    long epochMilliSeconds = 0;
    byte zero = 0;
    byte[] data = value.getData();
    switch (data.length) {
    case 4: {
      timestampReadBuffer.position(0);
      timestampReadBuffer.put(zero);
      timestampReadBuffer.put(zero);
      timestampReadBuffer.put(zero);
      timestampReadBuffer.put(zero);
      timestampReadBuffer.put(data);
      timestampReadBuffer.position(0);
      epochMilliSeconds = timestampReadBuffer.getLong() * 1000;
      break;
    }
    case 8: {
      timestampReadBuffer.position(0);
      timestampReadBuffer.put(data);
      timestampReadBuffer.position(0);
      long data64 = timestampReadBuffer.getLong();
      @SuppressWarnings("unused")
      long nanos = data64 >>> 34;
      long seconds = data64 & 0x00000003ffffffffL;
      epochMilliSeconds = (seconds * 1000) + (nanos / 1000000);
      break;
    }
    case 12: {
      timestampReadBuffer.position(0);
      timestampReadBuffer.put(data);
      timestampReadBuffer.position(0);
      int data32 = timestampReadBuffer.getInt();
      @SuppressWarnings("unused")
      long nanos = data32;
      long data64 = timestampReadBuffer.getLong();
      long seconds = data64;
      epochMilliSeconds = (seconds * 1000) + (nanos / 1000000);
      break;
    }
    default:
      logger.error("UnSupported built-in messagepack timestamp type (-1) with data length of: " + data.length);
    }

    if (mapWriter != null) {
      mapWriter.timeStamp(fieldName).writeTimeStamp(epochMilliSeconds);
    } else {
      listWriter.timeStamp().writeTimeStamp(epochMilliSeconds);
    }
  }

}
