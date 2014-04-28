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
package org.apache.drill.exec.store.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {
  static final ParseFilter FILTER_PARSEER = new ParseFilter();

  public static byte[] getBytes(String str) {
    return str == null ? HConstants.EMPTY_BYTE_ARRAY : Bytes.toBytes(str);
  }

  static Filter parseFilterString(String filterString) {
    if (filterString == null) return null;
    try {
        return FILTER_PARSEER.parseFilterString(filterString);
    } catch (CharacterCodingException e) {
      throw new DrillRuntimeException("Error parsing filter string: " + filterString, e);
    }
  }

  public static byte[] serializeFilter(Filter filter) {
    if (filter == null) return null;
    try(ByteArrayOutputStream byteStream = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(byteStream)) {
      HbaseObjectWritable.writeObject(out, filter, filter.getClass(), null);
      return byteStream.toByteArray();
    } catch (IOException e) {
      throw new DrillRuntimeException("Error serializing filter: " + filter, e);
    }
  }

  public static Filter deserializeFilter(byte[] filterBytes) {
    if (filterBytes == null) return null;
    try(DataInputStream dis = new DataInputStream(new ByteArrayInputStream(filterBytes));) {
      return (Filter) HbaseObjectWritable.readObject(dis, null);
    } catch (Exception e) {
      throw new DrillRuntimeException("Error deserializing filter: " + filterBytes, e);
    }
  }

}
