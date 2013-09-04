package org.apache.drill.exec.record.vector;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos.FieldDef;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableUInt4Vector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.junit.Test;

public class TestValueVector {

  DirectBufferAllocator allocator = new DirectBufferAllocator();

  @Test
  public void testFixedType() {
    // Build a required uint field definition
    MajorType.Builder typeBuilder = MajorType.newBuilder();
    FieldDef.Builder defBuilder = FieldDef.newBuilder();
    typeBuilder
        .setMinorType(MinorType.UINT4)
        .setMode(DataMode.REQUIRED)
        .setWidth(4);
    defBuilder
        .setMajorType(typeBuilder.build());
        MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    UInt4Vector v = new UInt4Vector(field, allocator);
    UInt4Vector.Mutator m = v.getMutator();
    v.allocateNew(1024);

    // Put and set a few values
    m.set(0, 100);
    m.set(1, 101);
    m.set(100, 102);
    m.set(1022, 103);
    m.set(1023, 104);
    assertEquals(100, v.getAccessor().get(0));
    assertEquals(101, v.getAccessor().get(1));
    assertEquals(102, v.getAccessor().get(100));
    assertEquals(103, v.getAccessor().get(1022));
    assertEquals(104, v.getAccessor().get(1023));

    // Ensure unallocated space returns 0
    assertEquals(0, v.getAccessor().get(3));
  }

  @Test
  public void testNullableVarLen2() {
    // Build an optional varchar field definition
    MajorType.Builder typeBuilder = MajorType.newBuilder();
    FieldDef.Builder defBuilder = FieldDef.newBuilder();
    typeBuilder
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .setWidth(2);
    defBuilder
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    NullableVarCharVector v = new NullableVarCharVector(field, allocator);
    NullableVarCharVector.Mutator m = v.getMutator();
    v.allocateNew(1024*10, 1024);

    // Create and set 3 sample strings
    String str1 = new String("AAAAA1");
    String str2 = new String("BBBBBBBBB2");
    String str3 = new String("CCCC3");
    m.set(0, str1.getBytes(Charset.forName("UTF-8")));
    m.set(1, str2.getBytes(Charset.forName("UTF-8")));
    m.set(2, str3.getBytes(Charset.forName("UTF-8")));

    // Check the sample strings
    assertEquals(str1, new String(v.getAccessor().get(0), Charset.forName("UTF-8")));
    assertEquals(str2, new String(v.getAccessor().get(1), Charset.forName("UTF-8")));
    assertEquals(str3, new String(v.getAccessor().get(2), Charset.forName("UTF-8")));

    // Ensure null value throws
    boolean b = false;
    try {
      v.getAccessor().get(3);
    } catch(AssertionError e) { 
      b = true;
    }finally{
      if(!b){
        assert false;
      }
    }

  }


  @Test
  public void testNullableFixedType() {
    // Build an optional uint field definition
    MajorType.Builder typeBuilder = MajorType.newBuilder();
    FieldDef.Builder defBuilder = FieldDef.newBuilder();
    typeBuilder
        .setMinorType(MinorType.UINT4)
        .setMode(DataMode.OPTIONAL)
        .setWidth(4);
    defBuilder
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    NullableUInt4Vector v = new NullableUInt4Vector(field, allocator);
    NullableUInt4Vector.Mutator m = v.getMutator();
    v.allocateNew(1024);

    // Put and set a few values
    m.set(0, 100);
    m.set(1, 101);
    m.set(100, 102);
    m.set(1022, 103);
    m.set(1023, 104);
    assertEquals(100, v.getAccessor().get(0));
    assertEquals(101, v.getAccessor().get(1));
    assertEquals(102, v.getAccessor().get(100));
    assertEquals(103, v.getAccessor().get(1022));
    assertEquals(104, v.getAccessor().get(1023));

    // Ensure null values throw
    {
      boolean b = false;
      try {
        v.getAccessor().get(3);
      } catch(AssertionError e) { 
        b = true;
      }finally{
        if(!b){
          assert false;
        }
      }      
    }

    
    v.allocateNew(2048);
    {
      boolean b = false;
      try {
        v.getAccessor().get(0);
      } catch(AssertionError e) { 
        b = true;
      }finally{
        if(!b){
          assert false;
        }
      }   
    }
    
    m.set(0, 100);
    m.set(1, 101);
    m.set(100, 102);
    m.set(1022, 103);
    m.set(1023, 104);
    assertEquals(100, v.getAccessor().get(0));
    assertEquals(101, v.getAccessor().get(1));
    assertEquals(102, v.getAccessor().get(100));
    assertEquals(103, v.getAccessor().get(1022));
    assertEquals(104, v.getAccessor().get(1023));

    // Ensure null values throw
    
    {
      boolean b = false;
      try {
        v.getAccessor().get(3);
      } catch(AssertionError e) { 
        b = true;
      }finally{
        if(!b){
          assert false;
        }
      }   
    }

  }

  @Test
  public void testNullableFloat() {
    // Build an optional float field definition
    MajorType.Builder typeBuilder = MajorType.newBuilder();
    FieldDef.Builder defBuilder = FieldDef.newBuilder();
    typeBuilder
        .setMinorType(MinorType.FLOAT4)
        .setMode(DataMode.OPTIONAL)
        .setWidth(4);
    defBuilder
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    NullableFloat4Vector v = (NullableFloat4Vector) TypeHelper.getNewVector(field, allocator);
    NullableFloat4Vector.Mutator m = v.getMutator();
    v.allocateNew(1024);

    // Put and set a few values
    m.set(0, 100.1f);
    m.set(1, 101.2f);
    m.set(100, 102.3f);
    m.set(1022, 103.4f);
    m.set(1023, 104.5f);
    assertEquals(100.1f, v.getAccessor().get(0), 0);
    assertEquals(101.2f, v.getAccessor().get(1), 0);
    assertEquals(102.3f, v.getAccessor().get(100), 0);
    assertEquals(103.4f, v.getAccessor().get(1022), 0);
    assertEquals(104.5f, v.getAccessor().get(1023), 0);

    // Ensure null values throw
    {
      boolean b = false;
      try {
        v.getAccessor().get(3);
      } catch(AssertionError e) { 
        b = true;
      }finally{
        if(!b){
          assert false;
        }
      }   
    }
    
    v.allocateNew(2048);
    {
      boolean b = false;
      try {
        v.getAccessor().get(0);
      } catch(AssertionError e) { 
        b = true;
      }finally{
        if(!b){
          assert false;
        }
      }   
    }
  }

  @Test
  public void testBitVector() {
    // Build a required boolean field definition
    MajorType.Builder typeBuilder = MajorType.newBuilder();
    FieldDef.Builder defBuilder = FieldDef.newBuilder();
    typeBuilder
        .setMinorType(MinorType.BIT)
        .setMode(DataMode.REQUIRED)
        .setWidth(4);
    defBuilder
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    BitVector v = new BitVector(field, allocator);
    BitVector.Mutator m = v.getMutator();
    v.allocateNew(1024);

    // Put and set a few values
    m.set(0, 1);
    m.set(1, 0);
    m.set(100, 0);
    m.set(1022, 1);
    assertEquals(1, v.getAccessor().get(0));
    assertEquals(0, v.getAccessor().get(1));
    assertEquals(0, v.getAccessor().get(100));
    assertEquals(1, v.getAccessor().get(1022));

    // test setting the same value twice
    m.set(0, 1);
    m.set(0, 1);
    m.set(1, 0);
    m.set(1, 0);
    assertEquals(1, v.getAccessor().get(0));
    assertEquals(0, v.getAccessor().get(1));

    // test toggling the values
    m.set(0, 0);
    m.set(1, 1);
    assertEquals(0, v.getAccessor().get(0));
    assertEquals(1, v.getAccessor().get(1));

    // Ensure unallocated space returns 0
    assertEquals(0, v.getAccessor().get(3));
  }

}
