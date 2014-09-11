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

import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestWriteToDisk  extends ExecTest{

  @Test
  public void test() throws Exception {
    List<ValueVector> vectorList = Lists.newArrayList();
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();
    Drillbit bit = new Drillbit(config, serviceSet);
    bit.run();
    DrillbitContext context = bit.getContext();

    MaterializedField intField = MaterializedField.create(new SchemaPath("int", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.INT));
    IntVector intVector = (IntVector)TypeHelper.getNewVector(intField, context.getAllocator());
    MaterializedField binField = MaterializedField.create(new SchemaPath("binary", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.VARBINARY));
    VarBinaryVector binVector = (VarBinaryVector)TypeHelper.getNewVector(binField, context.getAllocator());
    AllocationHelper.allocate(intVector, 4, 4);
    AllocationHelper.allocate(binVector, 4, 5);
    vectorList.add(intVector);
    vectorList.add(binVector);

    intVector.getMutator().setSafe(0, 0); binVector.getMutator().setSafe(0, "ZERO".getBytes());
    intVector.getMutator().setSafe(1, 1); binVector.getMutator().setSafe(1, "ONE".getBytes());
    intVector.getMutator().setSafe(2, 2); binVector.getMutator().setSafe(2, "TWO".getBytes());
    intVector.getMutator().setSafe(3, 3); binVector.getMutator().setSafe(3, "THREE".getBytes());
    intVector.getMutator().setValueCount(4);
    binVector.getMutator().setValueCount(4);

    VectorContainer container = new VectorContainer();
    container.addCollection(vectorList);
    container.setRecordCount(4);
    WritableBatch batch = WritableBatch.getBatchNoHVWrap(container.getRecordCount(), container, false);
    VectorAccessibleSerializable wrap = new VectorAccessibleSerializable(batch, context.getAllocator());

    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path("/tmp/drillSerializable");
    if (fs.exists(path)) {
      fs.delete(path, false);
    }
    FSDataOutputStream out = fs.create(path);

    wrap.writeToStream(out);
    out.close();

    FSDataInputStream in = fs.open(path);
    VectorAccessibleSerializable newWrap = new VectorAccessibleSerializable(context.getAllocator());
    newWrap.readFromStream(in);
    fs.close();

    VectorAccessible newContainer = newWrap.get();
    for (VectorWrapper w : newContainer) {
      ValueVector vv = w.getValueVector();
      int values = vv.getAccessor().getValueCount();
      for (int i = 0; i < values; i++) {
        Object o = vv.getAccessor().getObject(i);
        if (o instanceof byte[]) {
          System.out.println(new String((byte[])o));
        } else {
          System.out.println(o);
        }
      }
    }
  }

}
