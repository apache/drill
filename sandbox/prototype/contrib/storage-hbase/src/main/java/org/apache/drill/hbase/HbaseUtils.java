/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.hbase;

import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.UnsupportedEncodingException;

import static com.google.common.base.Throwables.propagate;

public class HbaseUtils {

  public static byte[] nameToBytes(CharSequence name) {
    try {
      return name.toString().getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw propagate(e);
    }
  }

  public static String nameFromBytes(byte[] bytes) {
    try {
      return new String(bytes, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw propagate(e);
    }
  }

  public static byte[] toBytes(DataValue dataValue) {
    if (dataValue instanceof ScalarValues.IntegerScalar) {
      return Bytes.toBytes(((ScalarValues.IntegerScalar) dataValue).getAsInt());
    }
    if (dataValue instanceof ScalarValues.LongScalar) {
      return Bytes.toBytes(((ScalarValues.LongScalar) dataValue).getAsLong());
    }
    if (dataValue instanceof ScalarValues.StringScalar) {
      return Bytes.toBytes(dataValue.toString());
    }
    if (dataValue instanceof ScalarValues.DoubleScalar) {
      return Bytes.toBytes(((ScalarValues.DoubleScalar) dataValue).getAsDouble());
    }
    throw new UnsupportedOperationException("Cannot transform : " + dataValue.getClass() + " into byte[]");
  }
}
