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
package org.apache.drill.exec.store.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import mockit.Expectations;
import mockit.Injectable;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;


public class JSONRecordReaderTest {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private String getResource(String resourceName) {
    return "resource:" + resourceName;
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

  private <T> void assertField(ValueVector valueVector, int index, MinorType expectedMinorType, T value, String name) {
    UserBitShared.FieldMetadata metadata = valueVector.getMetadata();
    SchemaDefProtos.FieldDef def = metadata.getDef();
    assertEquals(expectedMinorType, def.getMajorType().getMinorType());
    String[] parts = name.split("\\.");
    long expected = parts.length;
    assertEquals(expected, def.getNameList().size());
    for(int i = 0; i < parts.length; ++i) {
      assertEquals(parts[i], def.getName(i).getName());
    }

    if (expectedMinorType == MinorType.MAP) {
      return;
    }

    T val = (T) valueVector.getAccessor().getObject(index);
    assertValue(value, val);
  }

  private void assertValue(Object expected, Object found) {
    if (found instanceof byte[]) {
      assertTrue(Arrays.equals((byte[]) expected, (byte[]) found));
    } else if(found instanceof ArrayList) {
      List expectedArray = (List) expected;
      List foundArray = (List) found;
      assertEquals(expectedArray.size(), foundArray.size());
      for(int i = 0; i < expectedArray.size(); ++i) {
        assertValue(expectedArray.get(i), foundArray.get(i));
      }
    } else {
      assertEquals(expected, found);
    }
  }

  @Test
  public void testSameSchemaInSameBatch(@Injectable final FragmentContext context) throws IOException,
      ExecutionSetupException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new TopLevelAllocator());
      }
    };
    JSONRecordReader jr = new JSONRecordReader(context,
        FileUtils.getResourceAsFile("/scan_json_test_1.json").toURI().toString(),
        FileSystem.getLocal(new Configuration()), null, null);

    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();
    jr.setup(mutator);
    assertEquals(2, jr.next());
    assertEquals(3, addFields.size());
    assertField(addFields.get(0), 0, MinorType.BIGINT, 123L, "test");
    assertField(addFields.get(1), 0, MinorType.BIT, true, "b");
    assertField(addFields.get(2), 0, MinorType.VARCHAR, "hi!", "c");
    assertField(addFields.get(0), 1, MinorType.BIGINT, 1234L, "test");
    assertField(addFields.get(1), 1, MinorType.BIT, false, "b");
    assertField(addFields.get(2), 1, MinorType.VARCHAR, "drill!", "c");

    assertEquals(0, jr.next());
    assertTrue(mutator.getRemovedFields().isEmpty());
  }

  @Test
  public void testChangedSchemaInSameBatch(@Injectable final FragmentContext context) throws IOException,
      ExecutionSetupException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new TopLevelAllocator());
      }
    };

    JSONRecordReader jr = new JSONRecordReader(context,
        FileUtils.getResourceAsFile("/scan_json_test_2.json").toURI().toString(),
        FileSystem.getLocal(new Configuration()), null, null);

    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();

    jr.setup(mutator);
    assertEquals(3, jr.next());
    assertEquals(7, addFields.size());
    assertField(addFields.get(0), 0, MinorType.BIGINT, 123L, "test");
    assertField(addFields.get(1), 0, MinorType.BIGINT, 1L, "b");
    assertField(addFields.get(2), 0, MinorType.FLOAT4, (float) 2.15, "c");
    assertField(addFields.get(3), 0, MinorType.BIT, true, "bool");
    assertField(addFields.get(4), 0, MinorType.VARCHAR, "test1", "str1");

    assertField(addFields.get(0), 1, MinorType.BIGINT, 1234L, "test");
    assertField(addFields.get(1), 1, MinorType.BIGINT, 3L, "b");
    assertField(addFields.get(3), 1, MinorType.BIT, false, "bool");
    assertField(addFields.get(4), 1, MinorType.VARCHAR, "test2", "str1");
    assertField(addFields.get(5), 1, MinorType.BIGINT, 4L, "d");

    assertField(addFields.get(0), 2, MinorType.BIGINT, 12345L, "test");
    assertField(addFields.get(2), 2, MinorType.FLOAT4, (float) 5.16, "c");
    assertField(addFields.get(3), 2, MinorType.BIT, true, "bool");
    assertField(addFields.get(5), 2, MinorType.BIGINT, 6L, "d");
    assertField(addFields.get(6), 2, MinorType.VARCHAR, "test3", "str2");
    assertTrue(mutator.getRemovedFields().isEmpty());
    assertEquals(0, jr.next());
  }

  @Test
  public void testChangedSchemaInTwoBatchesColumnSelect(@Injectable final FragmentContext context) throws IOException,
      ExecutionSetupException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new TopLevelAllocator());
      }
    };

    JSONRecordReader jr = new JSONRecordReader(context,
        FileUtils.getResourceAsFile("/scan_json_test_2.json").toURI().toString(),
        FileSystem.getLocal(new Configuration()),
        64, null, Arrays.asList(new SchemaPath("test", ExpressionPosition.UNKNOWN))); // batch only fits 1 int
    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();
    List<MaterializedField> removedFields = mutator.getRemovedFields();

    jr.setup(mutator);
    assertEquals(1, jr.next());
    assertField(addFields.get(0), 0, MinorType.BIGINT, 123L, "test");
    assertTrue(removedFields.isEmpty());
    assertEquals(addFields.size(), 1);
    assertEquals(1, jr.next());
    assertField(addFields.get(0), 0, MinorType.BIGINT, 1234L, "test");
    assertEquals(addFields.size(), 1);
    assertTrue(removedFields.isEmpty());
    removedFields.clear();
    assertEquals(1, jr.next());
    assertField(addFields.get(0), 0, MinorType.BIGINT, 12345L, "test");
    assertEquals(addFields.size(), 1);
    assertTrue(removedFields.isEmpty());
    assertEquals(0, jr.next());
  }

  @Test
  public void testChangedSchemaInTwoBatches(@Injectable final FragmentContext context) throws IOException,
      ExecutionSetupException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new TopLevelAllocator());
      }
    };

    JSONRecordReader jr = new JSONRecordReader(context,
        FileUtils.getResourceAsFile("/scan_json_test_2.json").toURI().toString(),
        FileSystem.getLocal(new Configuration()),
        64, null, null); // batch only fits 1 int
    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();
    List<MaterializedField> removedFields = mutator.getRemovedFields();

    jr.setup(mutator);
    assertEquals(1, jr.next());
    assertEquals(5, addFields.size());
    assertField(addFields.get(0), 0, MinorType.BIGINT, 123L, "test");
    assertField(addFields.get(1), 0, MinorType.BIGINT, 1L, "b");
    assertField(addFields.get(2), 0, MinorType.FLOAT4, (float) 2.15, "c");
    assertField(addFields.get(3), 0, MinorType.BIT, true, "bool");
    assertField(addFields.get(4), 0, MinorType.VARCHAR, "test1", "str1");
    assertTrue(removedFields.isEmpty());
    assertEquals(1, jr.next());
    assertEquals(6, addFields.size());
    assertField(addFields.get(0), 0, MinorType.BIGINT, 1234L, "test");
    assertField(addFields.get(1), 0, MinorType.BIGINT, 3L, "b");
    assertField(addFields.get(3), 0, MinorType.BIT, false, "bool");
    assertField(addFields.get(4), 0, MinorType.VARCHAR, "test2", "str1");
    assertField(addFields.get(5), 0, MinorType.BIGINT, 4L, "d");
    assertEquals(1, removedFields.size());
    assertEquals("c", removedFields.get(0).getName());
    removedFields.clear();
    assertEquals(1, jr.next());
    assertEquals(7, addFields.size()); // The reappearing of field 'c' is also included
    assertField(addFields.get(0), 0, MinorType.BIGINT, 12345L, "test");
    assertField(addFields.get(3), 0, MinorType.BIT, true, "bool");
    assertField(addFields.get(5), 0, MinorType.BIGINT, 6L, "d");
    assertField(addFields.get(2), 0, MinorType.FLOAT4, (float) 5.16, "c");
    assertField(addFields.get(6), 0, MinorType.VARCHAR, "test3", "str2");
    assertEquals(2, removedFields.size());
    Iterables.find(removedFields, new Predicate<MaterializedField>() {
      @Override
      public boolean apply(MaterializedField materializedField) {
        return materializedField.getName().equals("str1");
      }
    });
    Iterables.find(removedFields, new Predicate<MaterializedField>() {
      @Override
      public boolean apply(MaterializedField materializedField) {
        return materializedField.getName().equals("b");
      }
    });
    assertEquals(0, jr.next());
  }

  @Test
  public void testNestedFieldInSameBatch(@Injectable final FragmentContext context) throws ExecutionSetupException, IOException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new TopLevelAllocator());
      }
    };

    JSONRecordReader jr = new JSONRecordReader(context,
        FileUtils.getResourceAsFile("/scan_json_test_3.json").toURI().toString(),
        FileSystem.getLocal(new Configuration()), null, null);

    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();
    jr.setup(mutator);
    assertEquals(2, jr.next());
    assertEquals(3, addFields.size());
    assertField(addFields.get(0), 0, MinorType.BIGINT, 123L, "test");
    assertField(addFields.get(1), 0, MinorType.VARCHAR, "test", "a.b");
    assertField(addFields.get(2), 0, MinorType.BIT, true, "a.a.d");
    assertField(addFields.get(0), 1, MinorType.BIGINT, 1234L, "test");
    assertField(addFields.get(1), 1, MinorType.VARCHAR, "test2", "a.b");
    assertField(addFields.get(2), 1, MinorType.BIT, false, "a.a.d");

    assertEquals(0, jr.next());
    assertTrue(mutator.getRemovedFields().isEmpty());
  }

  @Test
  public void testRepeatedFields(@Injectable final FragmentContext context) throws ExecutionSetupException, IOException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new TopLevelAllocator());
      }
    };

    JSONRecordReader jr = new JSONRecordReader(context,
        FileUtils.getResourceAsFile("/scan_json_test_4.json").toURI().toString(),
        FileSystem.getLocal(new Configuration()), null, null);

    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();
    jr.setup(mutator);
    assertEquals(2, jr.next());
    assertEquals(7, addFields.size());
    assertField(addFields.get(0), 0, MinorType.BIGINT, 123L, "test");
    assertField(addFields.get(1), 0, MinorType.BIGINT, Arrays.asList(1L, 2L, 3L), "test2");
    assertField(addFields.get(2), 0, MinorType.BIGINT, Arrays.asList(4L, 5L, 6L), "test3.a");
    assertField(addFields.get(3), 0, MinorType.BIGINT, Arrays.asList(7L, 8L, 9L), "test3.b");
    assertField(addFields.get(4), 0, MinorType.BIGINT, Arrays.asList(10L, 11L, 12L), "test3.c.d");
    assertField(addFields.get(5), 0, MinorType.FLOAT4, Arrays.<Float>asList((float) 1.1, (float) 1.2, (float) 1.3), "testFloat");
    assertField(addFields.get(6), 0, MinorType.VARCHAR, Arrays.asList("hello", "drill"), "testStr");
    assertField(addFields.get(1), 1, MinorType.BIGINT, Arrays.asList(1L, 2L), "test2");
    assertField(addFields.get(2), 1, MinorType.BIGINT, Arrays.asList(7L, 7L, 7L, 8L), "test3.a");
    assertField(addFields.get(5), 1, MinorType.FLOAT4, Arrays.<Float>asList((float) 2.2, (float) 2.3,(float) 2.4), "testFloat");

    assertEquals(0, jr.next());
    assertTrue(mutator.getRemovedFields().isEmpty());
  }

  @Test
  public void testRepeatedMissingFields(@Injectable final FragmentContext context) throws ExecutionSetupException, IOException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new TopLevelAllocator());
      }
    };

    JSONRecordReader jr = new JSONRecordReader(context,
        FileUtils.getResourceAsFile("/scan_json_test_5.json").toURI().toString(),
        FileSystem.getLocal(new Configuration()), null, null);

    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();
    jr.setup(mutator);
    assertEquals(9, jr.next());
    assertEquals(1, addFields.size());
    assertField(addFields.get(0), 0, MinorType.BIGINT, Arrays.<Long>asList(), "test");
    assertField(addFields.get(0), 1, MinorType.BIGINT, Arrays.asList(1L, 2L, 3L), "test");
    assertField(addFields.get(0), 2, MinorType.BIGINT, Arrays.<Long>asList(), "test");
    assertField(addFields.get(0), 3, MinorType.BIGINT, Arrays.<Long>asList(), "test");
    assertField(addFields.get(0), 4, MinorType.BIGINT, Arrays.asList(4L, 5L, 6L), "test");
    assertField(addFields.get(0), 5, MinorType.BIGINT, Arrays.<Long>asList(), "test");
    assertField(addFields.get(0), 6, MinorType.BIGINT, Arrays.<Long>asList(), "test");
    assertField(addFields.get(0), 7, MinorType.BIGINT, Arrays.asList(7L, 8L, 9L), "test");
    assertField(addFields.get(0), 8, MinorType.BIGINT, Arrays.<Long>asList(), "test");


    assertEquals(0, jr.next());
    assertTrue(mutator.getRemovedFields().isEmpty());
  }
}
