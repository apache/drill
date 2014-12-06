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
package org.apache.drill.exec.fn.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVar16CharVector;
import org.apache.drill.exec.vector.Var16CharVector;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class TestHiveUDFs extends BaseTestQuery {

  @Test
  public void testGenericUDF() throws Throwable {

    int numRecords = 0;
    String planString = Resources.toString(Resources.getResource("functions/hive/GenericUDF.json"), Charsets.UTF_8);
    List<QueryResultBatch> results = testPhysicalWithResults(planString);

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
    for (QueryResultBatch result : results) {
      batchLoader.load(result.getHeader().getDef(), result.getData());
      if (batchLoader.getRecordCount() <= 0) {
        result.release();
        batchLoader.clear();
        continue;
      }
      // Output columns and types
      //  1. str1 : Var16Char
      //  2. upperStr1 : NullableVar16Char
      //  3. concat : NullableVarChar
      //  4. flt1 : Float4
      //  5. format_number : NullableFloat8
      //  6. nullableStr1 : NullableVar16Char
      //  7. upperNullableStr1 : NullableVar16Char
      Var16CharVector str1V = (Var16CharVector) batchLoader.getValueAccessorById(Var16CharVector.class, 0).getValueVector();
      NullableVar16CharVector upperStr1V = (NullableVar16CharVector) batchLoader.getValueAccessorById(NullableVar16CharVector.class, 1).getValueVector();
      NullableVar16CharVector concatV = (NullableVar16CharVector) batchLoader.getValueAccessorById(NullableVar16CharVector.class, 2).getValueVector();
      Float4Vector flt1V = (Float4Vector) batchLoader.getValueAccessorById(Float4Vector.class, 3).getValueVector();
      NullableVar16CharVector format_numberV = (NullableVar16CharVector) batchLoader.getValueAccessorById(NullableVar16CharVector.class, 4).getValueVector();
      NullableVar16CharVector nullableStr1V = (NullableVar16CharVector) batchLoader.getValueAccessorById(NullableVar16CharVector.class, 5).getValueVector();
      NullableVar16CharVector upperNullableStr1V = (NullableVar16CharVector) batchLoader.getValueAccessorById(NullableVar16CharVector.class, 6).getValueVector();

      for (int i=0; i<batchLoader.getRecordCount(); i++) {
        String in = new String(str1V.getAccessor().get(i), Charsets.UTF_16);
        String upper = new String(upperStr1V.getAccessor().get(i), Charsets.UTF_16);
        assertTrue(in.toUpperCase().equals(upper));


        String concat = new String(concatV.getAccessor().get(i), Charsets.UTF_16);
        assertTrue(concat.equals(in+"-"+in));

        float flt1 = flt1V.getAccessor().get(i);
        String format_number = new String(format_numberV.getAccessor().get(i), Charsets.UTF_16);


        String nullableStr1 = null;
        if (!nullableStr1V.getAccessor().isNull(i)) {
          nullableStr1 = new String(nullableStr1V.getAccessor().get(i), Charsets.UTF_16);
        }

        String upperNullableStr1 = null;
        if (!upperNullableStr1V.getAccessor().isNull(i)) {
          upperNullableStr1 = new String(upperNullableStr1V.getAccessor().get(i), Charsets.UTF_16);
        }

        assertEquals(nullableStr1 != null, upperNullableStr1 != null);
        if (nullableStr1 != null) {
          assertEquals(nullableStr1.toUpperCase(), upperNullableStr1);
        }

        System.out.println(in + ", " + upper + ", " + concat + ", " +
          flt1 + ", " + format_number + ", " + nullableStr1 + ", " + upperNullableStr1);

        numRecords++;
      }

      result.release();
      batchLoader.clear();
    }

    System.out.println("Processed " + numRecords + " records");
  }

  @Test
  public void testUDF() throws Throwable {
    int numRecords = 0;
    String planString = Resources.toString(Resources.getResource("functions/hive/UDF.json"), Charsets.UTF_8);
    List<QueryResultBatch> results = testPhysicalWithResults(planString);

    RecordBatchLoader batchLoader = new RecordBatchLoader(getAllocator());
    for (QueryResultBatch result : results) {
      batchLoader.load(result.getHeader().getDef(), result.getData());
      if (batchLoader.getRecordCount() <= 0) {
        result.release();
        batchLoader.clear();
        continue;
      }

      // Output columns and types
      // 1. str1 : Var16Char
      // 2. str1Length : Int
      // 3. str1Ascii : Int
      // 4. flt1 : Float4
      // 5. pow : Float8
      Var16CharVector str1V = (Var16CharVector) batchLoader.getValueAccessorById(Var16CharVector.class, 0).getValueVector();
      NullableIntVector str1LengthV = (NullableIntVector) batchLoader.getValueAccessorById(NullableIntVector.class, 1).getValueVector();
      NullableIntVector str1AsciiV = (NullableIntVector) batchLoader.getValueAccessorById(NullableIntVector.class, 2).getValueVector();
      Float4Vector flt1V = (Float4Vector) batchLoader.getValueAccessorById(Float4Vector.class, 3).getValueVector();
      NullableFloat8Vector powV = (NullableFloat8Vector) batchLoader.getValueAccessorById(NullableFloat8Vector.class, 4).getValueVector();

      for (int i=0; i<batchLoader.getRecordCount(); i++) {
        String str1 = new String(str1V.getAccessor().get(i), Charsets.UTF_16);
        int str1Length = str1LengthV.getAccessor().get(i);
        assertTrue(str1.length() == str1Length);

        int str1Ascii = str1AsciiV.getAccessor().get(i);

        float flt1 = flt1V.getAccessor().get(i);

        double pow = 0;
        if (!powV.getAccessor().isNull(i)) {
          pow = powV.getAccessor().get(i);
          assertTrue(Math.pow(flt1, 2.0) == pow);
        }

        System.out.println(str1 + ", " + str1Length + ", " + str1Ascii + ", " + flt1 + ", " + pow);
        numRecords++;
      }

      result.release();
      batchLoader.clear();
    }

    System.out.println("Processed " + numRecords + " records");
  }

}
