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
package org.apache.drill.exec.cache;

import com.beust.jcommander.internal.Lists;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.hazelcast.nio.DataSerializable;
import io.netty.buffer.ByteBuf;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class VectorWrap implements DataSerializable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorWrap.class);

  List<ValueVector> vectorList;
  BootStrapContext context;
  final Parser<FieldMetadata> parser = FieldMetadata.PARSER;

  public VectorWrap(List<ValueVector> vectorList){
    this.vectorList = vectorList;
    this.context = new BootStrapContext(DrillConfig.create());
  }

  public VectorWrap() {
    this.vectorList = Lists.newArrayList();
    this.context = new BootStrapContext(DrillConfig.create());
  }
  
  @Override
  public void readData(DataInput arg0) throws IOException {
    int size = arg0.readInt();
    for (int i = 0; i < size; i++) {
      int metaLength = arg0.readInt();
      byte[] meta = new byte[metaLength];
      arg0.readFully(meta);
      FieldMetadata metaData = parser.parseFrom(meta);
      int dataLength = metaData.getBufferLength();
      byte[] bytes = new byte[dataLength];
      arg0.readFully(bytes);
      MaterializedField field = MaterializedField.create(metaData.getDef());
      ByteBuf buf = context.getAllocator().buffer(dataLength);
      buf.setBytes(0, bytes);
      ValueVector vector = TypeHelper.getNewVector(field, context.getAllocator());
      vector.load(metaData, buf);
      vectorList.add((BaseDataValueVector) vector);
    }
  }

  @Override
  public void writeData(DataOutput arg0) throws IOException {
    int size = vectorList.size();
    arg0.writeInt(size);
    for (ValueVector vector : vectorList) {
      byte[] meta = vector.getMetadata().toByteArray();
      int length = vector.getBufferSize();
      byte[] bytes = new byte[length];
      ByteBuf[] data = vector.getBuffers();
      int pos = 0;
      for (ByteBuf bb : data) {
        bb.getBytes(0, bytes, pos, bb.readableBytes());
        pos += bb.readableBytes();
      }
      arg0.writeInt(meta.length);
      arg0.write(meta);
      arg0.write(bytes);
    }
  }

  public List<ValueVector> get() {
    return vectorList;
  }

  public void set(List<ValueVector> vectorList) {
    this.vectorList = vectorList;
  }

}
