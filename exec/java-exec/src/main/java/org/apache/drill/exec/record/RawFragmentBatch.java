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
package org.apache.drill.exec.record;

import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.data.DataRpcConfig;

public class RawFragmentBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RawFragmentBatch.class);

  final RemoteConnection connection;
  final FragmentRecordBatch header;
  final DrillBuf body;
  final ResponseSender sender;

  public RawFragmentBatch(RemoteConnection connection, FragmentRecordBatch header, DrillBuf body, ResponseSender sender) {
    super();
    this.header = header;
    this.body = body;
    this.connection = connection;
    this.sender = sender;
    if (body != null) {
      body.retain();
    }
  }

  public FragmentRecordBatch getHeader() {
    return header;
  }

  public DrillBuf getBody() {
    return body;
  }

  @Override
  public String toString() {
    return "RawFragmentBatch [header=" + header + ", body=" + body + "]";
  }

  public void release() {
    if (body != null) {
      body.release();
    }
  }

  public RemoteConnection getConnection() {
    return connection;
  }

  public ResponseSender getSender() {
    return sender;
  }

  public void sendOk() {
    sender.send(DataRpcConfig.OK);
  }

  public long getByteCount() {
    return body == null ? 0 : body.readableBytes();
  }

}
