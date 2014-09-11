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
package org.apache.drill.exec.rpc.user;

import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.proto.UserBitShared.QueryResult;

public class QueryResultBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryResultBatch.class);

  private final QueryResult header;
  private final DrillBuf data;

  public QueryResultBatch(QueryResult header, DrillBuf data) {
//    logger.debug("New Result Batch with header {} and data {}", header, data);
    this.header = header;
    this.data = data;
    if (this.data != null) {
      data.retain();
    }
  }

  public QueryResult getHeader() {
    return header;
  }

  public DrillBuf getData() {
    return data;
  }

  public boolean hasData() {
    return data != null;
  }

  public void release() {
    if (data != null) {
      data.release();
    }
  }

  @Override
  public String toString() {
    return "QueryResultBatch [header=" + header + ", data=" + data + "]";
  }

}
