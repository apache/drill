/*
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

import java.io.File;
import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.test.TestTools;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.junit.rules.TestRule;

public class TestWriteToDisk extends ExecTest {
  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(90000); // 90secs

  @Test
  @SuppressWarnings("static-method")
  public void test() throws Exception {
    final List<ValueVector> vectorList = Lists.newArrayList();
    final DrillConfig config = DrillConfig.create();
    try (final RemoteServiceSet serviceSet = RemoteServiceSet
        .getLocalServiceSet();
        final Drillbit bit = new Drillbit(config, serviceSet)) {
      bit.run();
      final DrillbitContext context = bit.getContext();

      final MaterializedField intField = MaterializedField.create("int", Types.required(TypeProtos.MinorType.INT));
      final MaterializedField binField = MaterializedField.create("binary", Types.required(TypeProtos.MinorType.VARBINARY));
      try (final IntVector intVector = (IntVector) TypeHelper.getNewVector(intField, context.getAllocator());
          final VarBinaryVector binVector =
              (VarBinaryVector) TypeHelper.getNewVector(binField, context.getAllocator())) {
        AllocationHelper.allocate(intVector, 4, 4);
        AllocationHelper.allocate(binVector, 4, 5);
        vectorList.add(intVector);
        vectorList.add(binVector);

        intVector.getMutator().setSafe(0, 0);
        binVector.getMutator().setSafe(0, "ZERO".getBytes());
        intVector.getMutator().setSafe(1, 1);
        binVector.getMutator().setSafe(1, "ONE".getBytes());
        intVector.getMutator().setSafe(2, 2);
        binVector.getMutator().setSafe(2, "TWO".getBytes());
        intVector.getMutator().setSafe(3, 3);
        binVector.getMutator().setSafe(3, "THREE".getBytes());
        intVector.getMutator().setValueCount(4);
        binVector.getMutator().setValueCount(4);

        VectorContainer container = new VectorContainer();
        container.addCollection(vectorList);
        container.setRecordCount(4);
        @SuppressWarnings("resource")
        WritableBatch batch = WritableBatch.getBatchNoHVWrap(
            container.getRecordCount(), container, false);
        VectorAccessibleSerializable wrap = new VectorAccessibleSerializable(
            batch, context.getAllocator());

        final VectorAccessibleSerializable newWrap = new VectorAccessibleSerializable(
            context.getAllocator());
        try (final FileSystem fs = getLocalFileSystem()) {
          final File tempDir = Files.createTempDir();
          tempDir.deleteOnExit();
          final Path path = new Path(tempDir.getAbsolutePath(), "drillSerializable");
          try (final FSDataOutputStream out = fs.create(path)) {
            wrap.writeToStream(out);
          }

          try (final FSDataInputStream in = fs.open(path)) {
            newWrap.readFromStream(in);
          }
        }

        newWrap.get();
      }
    }
  }
}
