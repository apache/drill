package org.apache.drill.exec.record.vector;

import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.io.UTF8;
import org.junit.Test;

import java.nio.charset.Charset;

public class TestValueVector {

  DirectBufferAllocator allocator = new DirectBufferAllocator();

  @Test
  public void testFixedType() {
    // Build a required uint field definition
    SchemaDefProtos.MajorType.Builder typeBuilder = SchemaDefProtos.MajorType.newBuilder();
    SchemaDefProtos.FieldDef.Builder defBuilder = SchemaDefProtos.FieldDef.newBuilder();
    typeBuilder
        .setMinorType(SchemaDefProtos.MinorType.UINT4)
        .setMode(SchemaDefProtos.DataMode.REQUIRED)
        .setWidth(4);
    defBuilder
        .setFieldId(1)
        .setParentId(0)
        .setMajorType(typeBuilder.build());
        MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    ValueVector.MutableUInt4 v = new ValueVector.MutableUInt4(field, allocator);
    v.allocateNew(1024);

    // Put and set a few values
    v.set(0, 100);
    v.set(1, 101);
    v.set(100, 102);
    v.set(1022, 103);
    v.set(1023, 104);
    assertEquals(100, v.get(0));
    assertEquals(101, v.get(1));
    assertEquals(102, v.get(100));
    assertEquals(103, v.get(1022));
    assertEquals(104, v.get(1023));

    // Ensure unallocated space returns 0
    assertEquals(0, v.get(3));
  }

  @Test
  public void testNullableVarLen2() {
    // Build an optional varchar field definition
    SchemaDefProtos.MajorType.Builder typeBuilder = SchemaDefProtos.MajorType.newBuilder();
    SchemaDefProtos.FieldDef.Builder defBuilder = SchemaDefProtos.FieldDef.newBuilder();
    typeBuilder
        .setMinorType(SchemaDefProtos.MinorType.VARCHAR2)
        .setMode(SchemaDefProtos.DataMode.OPTIONAL)
        .setWidth(2);
    defBuilder
        .setFieldId(1)
        .setParentId(0)
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    ValueVector.NullableVarChar2 v = new ValueVector.NullableVarChar2(field, allocator);
    v.allocateNew(1024);

    // Create and set 3 sample strings
    String str1 = new String("AAAAA1");
    String str2 = new String("BBBBBBBBB2");
    String str3 = new String("CCCC3");
    v.set(0, str1.getBytes(Charset.forName("UTF-8")));
    v.set(1, str2.getBytes(Charset.forName("UTF-8")));
    v.set(2, str3.getBytes(Charset.forName("UTF-8")));

    // Check the sample strings
    assertEquals(str1, new String(v.get(0), Charset.forName("UTF-8")));
    assertEquals(str2, new String(v.get(1), Charset.forName("UTF-8")));
    assertEquals(str3, new String(v.get(2), Charset.forName("UTF-8")));

    // Ensure null value throws
    try {
      v.get(3);
      assertFalse(false);
    } catch(NullValueException e) { }

  }


  @Test
  public void testNullableFixedType() {
    // Build an optional uint field definition
    SchemaDefProtos.MajorType.Builder typeBuilder = SchemaDefProtos.MajorType.newBuilder();
    SchemaDefProtos.FieldDef.Builder defBuilder = SchemaDefProtos.FieldDef.newBuilder();
    typeBuilder
        .setMinorType(SchemaDefProtos.MinorType.UINT4)
        .setMode(SchemaDefProtos.DataMode.OPTIONAL)
        .setWidth(4);
    defBuilder
        .setFieldId(1)
        .setParentId(0)
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    ValueVector.NullableUInt4 v = new ValueVector.NullableUInt4(field, allocator);
    v.allocateNew(1024);

    // Put and set a few values
    v.set(0, 100);
    v.set(1, 101);
    v.set(100, 102);
    v.set(1022, 103);
    v.set(1023, 104);
    assertEquals(100, v.get(0));
    assertEquals(101, v.get(1));
    assertEquals(102, v.get(100));
    assertEquals(103, v.get(1022));
    assertEquals(104, v.get(1023));

    // Ensure null values throw
    try {
      v.get(3);
      assertFalse(false);
    } catch(NullValueException e) { }

    v.allocateNew(2048);
    try {
      v.get(0);
      assertFalse(false);
    } catch(NullValueException e) { }

    v.set(0, 100);
    v.set(1, 101);
    v.set(100, 102);
    v.set(1022, 103);
    v.set(1023, 104);
    assertEquals(100, v.get(0));
    assertEquals(101, v.get(1));
    assertEquals(102, v.get(100));
    assertEquals(103, v.get(1022));
    assertEquals(104, v.get(1023));

    // Ensure null values throw
    try {
      v.get(3);
      assertFalse(false);
    } catch(NullValueException e) { }

  }

  @Test
  public void testNullableFloat() {
    // Build an optional float field definition
    SchemaDefProtos.MajorType.Builder typeBuilder = SchemaDefProtos.MajorType.newBuilder();
    SchemaDefProtos.FieldDef.Builder defBuilder = SchemaDefProtos.FieldDef.newBuilder();
    typeBuilder
        .setMinorType(SchemaDefProtos.MinorType.FLOAT4)
        .setMode(SchemaDefProtos.DataMode.OPTIONAL)
        .setWidth(4);
    defBuilder
        .setFieldId(1)
        .setParentId(0)
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    ValueVector.NullableFloat4 v = (ValueVector.NullableFloat4) TypeHelper.getNewVector(field, allocator);

    v.allocateNew(1024);

    // Put and set a few values
    v.set(0, 100.1f);
    v.set(1, 101.2f);
    v.set(100, 102.3f);
    v.set(1022, 103.4f);
    v.set(1023, 104.5f);
    assertEquals(100.1f, v.get(0), 0);
    assertEquals(101.2f, v.get(1), 0);
    assertEquals(102.3f, v.get(100), 0);
    assertEquals(103.4f, v.get(1022), 0);
    assertEquals(104.5f, v.get(1023), 0);

    // Ensure null values throw
    try {
      v.get(3);
      assertFalse(false);
    } catch(NullValueException e) { }

    v.allocateNew(2048);
    try {
      v.get(0);
      assertFalse(false);
    } catch(NullValueException e) { }

  }

  @Test
  public void testBitVector() {
    // Build a required boolean field definition
    SchemaDefProtos.MajorType.Builder typeBuilder = SchemaDefProtos.MajorType.newBuilder();
    SchemaDefProtos.FieldDef.Builder defBuilder = SchemaDefProtos.FieldDef.newBuilder();
    typeBuilder
        .setMinorType(SchemaDefProtos.MinorType.BOOLEAN)
        .setMode(SchemaDefProtos.DataMode.REQUIRED)
        .setWidth(4);
    defBuilder
        .setFieldId(1)
        .setParentId(0)
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    ValueVector.MutableBit v = new ValueVector.MutableBit(field, allocator);
    v.allocateNew(1024);

    // Put and set a few values
    v.set(0, 1);
    v.set(1, 0);
    v.set(100, 0);
    v.set(1022, 1);
    assertEquals(1, v.get(0));
    assertEquals(0, v.get(1));
    assertEquals(0, v.get(100));
    assertEquals(1, v.get(1022));

    // test setting the same value twice
    v.set(0, 1);
    v.set(0, 1);
    v.set(1, 0);
    v.set(1, 0);
    assertEquals(1, v.get(0));
    assertEquals(0, v.get(1));

    // test toggling the values
    v.set(0, 0);
    v.set(1, 1);
    assertEquals(0, v.get(0));
    assertEquals(1, v.get(1));

    // Ensure unallocated space returns 0
    assertEquals(0, v.get(3));
  }

}
