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

import io.netty.buffer.ByteBuf;

import java.util.List;

import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;

import com.google.common.collect.Lists;

public class FragmentWritableBatch{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentWritableBatch.class);

  private static RecordBatchDef EMPTY_DEF = RecordBatchDef.newBuilder().setRecordCount(0).build();

  private final ByteBuf[] buffers;
  private final FragmentRecordBatch header;

  public FragmentWritableBatch(boolean isLast, QueryId queryId, int sendMajorFragmentId, int sendMinorFragmentId, int receiveMajorFragmentId, int receiveMinorFragmentId, WritableBatch batch){
    this(isLast, queryId, sendMajorFragmentId, sendMinorFragmentId, receiveMajorFragmentId, receiveMinorFragmentId, batch.getDef(), batch.getBuffers());
  }

  private FragmentWritableBatch(boolean isLast, QueryId queryId, int sendMajorFragmentId, int sendMinorFragmentId, int receiveMajorFragmentId, int receiveMinorFragmentId, RecordBatchDef def, ByteBuf... buffers){
    this.buffers = buffers;
    FragmentHandle handle = FragmentHandle //
        .newBuilder() //
        .setMajorFragmentId(receiveMajorFragmentId) //
        .setMinorFragmentId(receiveMinorFragmentId) //
        .setQueryId(queryId) //
        .build();
    this.header = FragmentRecordBatch //
        .newBuilder() //
        .setIsLastBatch(isLast) //
        .setDef(def) //
        .setHandle(handle) //
        .setSendingMajorFragmentId(sendMajorFragmentId) //
        .setSendingMinorFragmentId(sendMinorFragmentId) //
        .build();
  }


  public static FragmentWritableBatch getEmptyLast(QueryId queryId, int sendMajorFragmentId, int sendMinorFragmentId, int receiveMajorFragmentId, int receiveMinorFragmentId){
    return new FragmentWritableBatch(true, queryId, sendMajorFragmentId, sendMinorFragmentId, receiveMajorFragmentId, receiveMinorFragmentId, EMPTY_DEF);
  }

  public static FragmentWritableBatch getEmptyLastWithSchema(QueryId queryId, int sendMajorFragmentId, int sendMinorFragmentId,
                                                             int receiveMajorFragmentId, int receiveMinorFragmentId, BatchSchema schema){
    return getEmptyBatchWithSchema(true, queryId, sendMajorFragmentId, sendMinorFragmentId, receiveMajorFragmentId,
        receiveMinorFragmentId, schema);
  }

  public static FragmentWritableBatch getEmptyBatchWithSchema(boolean isLast, QueryId queryId, int sendMajorFragmentId,
      int sendMinorFragmentId, int receiveMajorFragmentId, int receiveMinorFragmentId, BatchSchema schema){

    List<SerializedField> fields = Lists.newArrayList();
    for (MaterializedField field : schema) {
      fields.add(field.getSerializedField());
    }
    RecordBatchDef def = RecordBatchDef.newBuilder().addAllField(fields).build();
    return new FragmentWritableBatch(isLast, queryId, sendMajorFragmentId, sendMinorFragmentId, receiveMajorFragmentId,
        receiveMinorFragmentId, def);
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

  public FragmentRecordBatch getHeader() {
    return header;

  }





}
