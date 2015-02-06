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
package org.apache.drill.exec.physical.impl.materialize;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.List;

import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;

import com.google.common.collect.Lists;

public class QueryWritableBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryWritableBatch.class);

  private final QueryResult header;
  private final ByteBuf[] buffers;


  public QueryWritableBatch(QueryResult header, ByteBuf... buffers) {
    super();
    this.header = header;
    this.buffers = buffers;
  }

  public ByteBuf[] getBuffers(){
    return buffers;
  }

  public long getByteCount() {
    long n = 0;
    for (ByteBuf buf : buffers) {
      n += buf.readableBytes();
    }
    return n;
  }

  public QueryResult getHeader() {
    return header;
  }

  @Override
  public String toString() {
    return "QueryWritableBatch [header=" + header + ", buffers=" + Arrays.toString(buffers) + "]";
  }
}
