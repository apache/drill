package org.apache.drill.exec.store;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.orc.OrcRecordReader;
import org.apache.drill.exec.vector.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class OrcRecordReaderTest {
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  Path workDir = new Path("target" + File.separator + "test" + File.separator + "tmp");

  public static class Struct {
    public Integer i;
    public String t;
    public Double d;
    public Float f;
    public Boolean b;
    public Short s;

    public Struct() {
      Random random = new Random();

      if(random.nextBoolean())
        i = random.nextInt();

      if(random.nextBoolean())
        t = "test" + random.nextInt();

      if(random.nextBoolean())
        d = random.nextDouble();

      if(random.nextBoolean())
        f = random.nextFloat();

      if(random.nextBoolean())
        b = random.nextBoolean();

      if(random.nextBoolean())
        s = (short)random.nextInt();
    }
  }

  class MockOutputMutator implements OutputMutator {
    List<MaterializedField> removedFields = Lists.newArrayList();
    List<ValueVector> addFields = Lists.newArrayList();

    @Override
    public void removeField(MaterializedField field) throws SchemaChangeException {
      removedFields.add(field);
    }

    @Override
    public void addField(ValueVector vector) throws SchemaChangeException {
      addFields.add(vector);
    }

    @Override
    public void removeAllFields() {
      addFields.clear();
    }

    @Override
    public void setNewSchema() throws SchemaChangeException {
    }

    List<MaterializedField> getRemovedFields() {
      return removedFields;
    }

    List<ValueVector> getAddFields() {
      return addFields;
    }
  }

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void shouldParseFlatOrcFile(@Injectable final FragmentContext context) throws IOException, ExecutionSetupException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new TopLevelAllocator());
      }
    };
    ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector
        (Struct.class,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        1000, CompressionKind.NONE, 100, 1000);
    List<Struct> rows = Lists.newArrayList();
    for(int i = 0; i < 100; ++i) {
      Struct row = new Struct();
      writer.addRow(row);
      rows.add(row);
    }
    writer.close();

    OrcRecordReader reader = new OrcRecordReader(context, fs, testFilePath, 128 * 1024);
    MockOutputMutator mutator = new MockOutputMutator();
    reader.setup(mutator);
    assertEquals(rows.size(), reader.next());
    List<ValueVector> vv = mutator.getAddFields();
    for(int i = 0; i < rows.size(); ++i) {
      assertRow(vv, rows.get(i), i);
    }
  }

  private void assertRow(List<ValueVector> valueVector, Struct struct, int row) {
    NullableIntVector vv1 = (NullableIntVector) valueVector.get(0);
    assertEquals("i", vv1.getField().getName());
    NullableVarCharVector vv2 = (NullableVarCharVector) valueVector.get(1);
    assertEquals("t", vv2.getField().getName());
    NullableFloat8Vector vv3 = (NullableFloat8Vector) valueVector.get(2);
    assertEquals("d", vv3.getField().getName());
    NullableFloat4Vector vv4 = (NullableFloat4Vector) valueVector.get(3);
    assertEquals("f", vv4.getField().getName());
    NullableBitVector vv5 = (NullableBitVector) valueVector.get(4);
    assertEquals("b", vv5.getField().getName());
    NullableSmallIntVector vv6 = (NullableSmallIntVector) valueVector.get(5);
    assertEquals("s", vv6.getField().getName());

    assertEquals(struct.i == null ? 0 : 1, vv1.getBits().getAccessor().get(row));
    if(struct.i != null)
      assertEquals((int) struct.i, vv1.getAccessor().get(row));

    assertEquals(struct.t == null ? 0 : 1, vv2.getBits().getAccessor().get(row));
    if(struct.t != null)
      assertEquals(struct.t.toString(), UTF_8.decode(ByteBuffer.wrap(vv2.getAccessor().get(row))).toString());

    assertEquals(struct.d == null ? 0 : 1, vv3.getBits().getAccessor().get(row));
    if(struct.d != null)
      assertEquals(struct.d, vv3.getAccessor().get(row), 0.001);

    assertEquals(struct.f == null ? 0 : 1, vv4.getBits().getAccessor().get(row));
    if(struct.f != null)
      assertEquals(struct.f, vv4.getAccessor().get(row), 0.001);

    assertEquals(struct.b == null ? 0 : 1, vv5.getBits().getAccessor().get(row));
    if(struct.b != null)
      assertEquals(struct.b, vv5.getAccessor().get(row) == 1);

    assertEquals(struct.s == null ? 0 : 1, vv6.getBits().getAccessor().get(row));
    if(struct.s != null)
      assertEquals((short)struct.s, vv6.getAccessor().get(row));
  }
}
