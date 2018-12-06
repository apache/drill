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
package org.apache.drill.exec.store.msgpack;

import static org.apache.drill.exec.record.MaterializedField.create;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Stack;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeRuntimeException;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.msgpack.schema.MsgpackSchema;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MsgpackSchemaTest extends ClusterTest {
  private static DrillFileSystem dfs;
  private static MsgpackSchema msgpackSchema;
  private static Path dirLocation;

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    dfs = new DrillFileSystem(conf);
    File tempDir = dirTestWatcher.getTmpDir();
    dirLocation = new Path(tempDir.getAbsolutePath());
    msgpackSchema = new MsgpackSchema(dfs, dirLocation);
  }

  @Before
  public void before() throws Exception {
    msgpackSchema.delete();
  }

  @Test
  public void noSchemaLoadReturnsNull() throws Exception {
    msgpackSchema.load();
    assertNull(msgpackSchema.getSchema());
  }

  @Test
  public void samePrevMergesWithNoEffect() throws Exception {
    MaterializedField newMapField = new Builder().addBigInt("x").build();
    msgpackSchema.save(newMapField);
    MaterializedField existingField = msgpackSchema.load().getSchema();
    assertNotNull(existingField);
    MaterializedField sameNewMapField = new Builder().addBigInt("x").build();
    assertTrue(existingField.isEquivalent(sameNewMapField));
    MaterializedField merge = msgpackSchema.merge(existingField, sameNewMapField);
    assertTrue(existingField.isEquivalent(merge));
  }

  @Test
  public void newMapFieldIsAdded() throws Exception {
    MaterializedField newMapField = new Builder().addBigInt("x").build();
    msgpackSchema.save(newMapField);
    MaterializedField existingField = msgpackSchema.load().getSchema();
    assertNotNull(existingField);
    MaterializedField differentNewMapField = new Builder().addVarChar("y").build();
    assertFalse(existingField.isEquivalent(differentNewMapField));
    MaterializedField merge = msgpackSchema.merge(existingField, differentNewMapField);
    assertNotNull(merge);
    assertFalse(existingField.isEquivalent(merge));
    MaterializedField expected = new Builder().addBigInt("x").addVarChar("y").build();
    assertTrue(merge.isEquivalent(expected));
  }

  @Test(expected = SchemaChangeRuntimeException.class)
  public void sameFieldNameDifferentTypeProducesError() throws Exception {
    MaterializedField newMapField = new Builder().addBigInt("x").build();
    msgpackSchema.save(newMapField);
    MaterializedField existingField = msgpackSchema.load().getSchema();
    assertNotNull(existingField);
    MaterializedField differentNewMapField = new Builder().addVarChar("x").build();
    assertFalse(existingField.isEquivalent(differentNewMapField));
    MaterializedField merge = msgpackSchema.merge(existingField, differentNewMapField);
  }

  public static class Builder {
    MajorType mapType = MajorType.newBuilder().setMode(DataMode.REQUIRED).setMinorType(MinorType.MAP).build();
    MajorType bigIntType = MajorType.newBuilder().setMode(DataMode.OPTIONAL).setMinorType(MinorType.BIGINT).build();
    MajorType varCharType = MajorType.newBuilder().setMode(DataMode.OPTIONAL).setMinorType(MinorType.VARCHAR).build();
    MajorType repeatedFloatType = MajorType.newBuilder().setMode(DataMode.REPEATED).setMinorType(MinorType.FLOAT8)
        .build();
    MajorType repeatedMapType = MajorType.newBuilder().setMode(DataMode.REPEATED).setMinorType(MinorType.MAP).build();
    private Stack<MaterializedField> stack = new Stack<>();

    public Builder() {
      MaterializedField root = create("", mapType);
      stack.push(root);
    }

    public MaterializedField build() {
      return stack.pop();
    }

    public Builder startMap(String name) {
      MaterializedField aMap = create(name, mapType);
      stack.push(aMap);
      return this;
    }

    public Builder endMap(String name) {
      stack.pop();
      return this;
    }

    private Builder add(String name, MajorType type) {
      if (type.getMinorType() == MinorType.MAP) {
        throw new UnsupportedOperationException("Use startMap to add a map");
      }

      MaterializedField child = create(name, type);
      stack.peek().addChild(child);
      return this;
    }

    public Builder addBigInt(String name) {
      return add(name, bigIntType);
    }

    public Builder addVarChar(String name) {
      return add(name, varCharType);
    }
  }
}
