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
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MsgpackSchemaTest extends ClusterTest {
  private static DrillFileSystem dfs;
  private static File tempDir;
  private static Path tempPath;
  private static Path schemaLocation;
  private MsgpackSchema msgpackSchema;

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    dfs = new DrillFileSystem(conf);
    tempDir = dirTestWatcher.getTmpDir();
    tempPath = new Path(tempDir.getAbsolutePath());
    schemaLocation = new Path(tempPath, "test.proto.schema");
  }

  @Before
  public void before() throws Exception {
    dfs.delete(schemaLocation, false);
    msgpackSchema = new MsgpackSchema(dfs);
  }

  @Test
  public void noSchemaLoadReturnsNull() throws Exception {
    MaterializedField load = msgpackSchema.load(schemaLocation);
    assertNull(load);
  }

  @Test
  public void samePrevMergesWithNoEffect() throws Exception {
    MaterializedField newMapField = new Builder().addBigInt("x").build();
    msgpackSchema.save(newMapField, schemaLocation);
    MaterializedField existingField = msgpackSchema.load(schemaLocation);
    assertNotNull(existingField);
    MaterializedField sameNewMapField = new Builder().addBigInt("x").build();
    assertTrue(existingField.isEquivalent(sameNewMapField));
    MaterializedField merge = msgpackSchema.merge(existingField, sameNewMapField);
    assertTrue(existingField.isEquivalent(merge));
  }

  @Test
  public void newMapFieldIsAdded() throws Exception {
    MaterializedField newMapField = new Builder().addBigInt("x").build();
    msgpackSchema.save(newMapField, schemaLocation);
    MaterializedField existingField = msgpackSchema.load(schemaLocation);
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
    msgpackSchema.save(newMapField, schemaLocation);
    MaterializedField existingField = msgpackSchema.load(schemaLocation);
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
