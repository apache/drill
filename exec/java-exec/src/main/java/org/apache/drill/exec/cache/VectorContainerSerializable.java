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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.DataInputInputStream;
import org.apache.drill.common.util.DataOutputOutputStream;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.server.BootStrapContext;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;

import java.io.*;
import java.util.List;

public class VectorContainerSerializable implements DrillSerializable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorContainerSerializable.class);

//  List<ValueVector> vectorList;
  private VectorContainer container;
  private BootStrapContext context;
  private int listSize = 0;
  private int recordCount = -1;

  /**
   *
   * @param container
   */
  public VectorContainerSerializable(VectorContainer container){
    this.container = container;
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    this.context = new BootStrapContext(DrillConfig.create());
    this.listSize = container.getNumberOfColumns();
  }

  public VectorContainerSerializable() {
    this.container = new VectorContainer();
    this.context = new BootStrapContext(DrillConfig.create());
  }
  
  @Override
  public void read(DataInput input) throws IOException {
    List<ValueVector> vectorList = Lists.newArrayList();
    int size = input.readInt();
    InputStream stream = DataInputInputStream.constructInputStream(input);
    for (int i = 0; i < size; i++) {
      FieldMetadata metaData = FieldMetadata.parseDelimitedFrom(stream);
      if (recordCount == -1) recordCount = metaData.getValueCount();
      int dataLength = metaData.getBufferLength();
      byte[] bytes = new byte[dataLength];
      input.readFully(bytes);
      MaterializedField field = MaterializedField.create(metaData.getDef());
      ByteBuf buf = context.getAllocator().buffer(dataLength);
      buf.setBytes(0, bytes);
      ValueVector vector = TypeHelper.getNewVector(field, context.getAllocator());
      vector.load(metaData, buf);
      vectorList.add((BaseDataValueVector) vector);
    }
    container.addCollection(vectorList);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    container.setRecordCount(recordCount);
    this.listSize = vectorList.size();
  }

  @Override
  public void write(DataOutput output) throws IOException {
//    int size = vectorList.size();
    output.writeInt(listSize);
    for (VectorWrapper w : container) {
      OutputStream stream = DataOutputOutputStream.constructOutputStream(output);
      ValueVector vector = w.getValueVector();
      if (recordCount == -1) container.setRecordCount(vector.getMetadata().getValueCount());
      vector.getMetadata().writeDelimitedTo(stream);
      ByteBuf[] data = vector.getBuffers();
      for (ByteBuf bb : data) {
        byte[] bytes = new byte[bb.readableBytes()];
        bb.getBytes(0, bytes);
        stream.write(bytes);
      }
    }
  }

  public VectorContainer get() {
    return container;
  }
}
