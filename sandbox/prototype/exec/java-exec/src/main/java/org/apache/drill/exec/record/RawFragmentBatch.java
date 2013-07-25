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
package org.apache.drill.exec.record;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.proto.ExecProtos.FragmentRecordBatch;

public class RawFragmentBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RawFragmentBatch.class);

  final FragmentRecordBatch header;
  final ByteBuf body;

  public RawFragmentBatch(FragmentRecordBatch header, ByteBuf body) {
    super();
    this.header = header;
    this.body = body;
    if(body != null) body.retain();
  }

  public FragmentRecordBatch getHeader() {
    return header;
  }

  public ByteBuf getBody() {
    return body;
  }

  @Override
  public String toString() {
    return "RawFragmentBatch [header=" + header + ", body=" + body + "]";
  }

}
