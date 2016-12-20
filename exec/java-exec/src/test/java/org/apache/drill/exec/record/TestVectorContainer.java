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
package org.apache.drill.exec.record;

import static org.junit.Assert.*;

import org.apache.drill.categories.VectorTest;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(VectorTest.class)
public class TestVectorContainer extends DrillTest {

  // TODO: Replace the following with an extension of SubOperatorTest class
  // once that is available.

  protected static OperatorFixture fixture;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    fixture = OperatorFixture.standardFixture();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fixture.close();
  }

  /**
   * Test of the ability to merge two schemas and to merge
   * two vector containers. The merge is "horizontal", like
   * a row-by-row join. Since each container is a list of
   * vectors, we just combine the two lists to create the
   * merged result.
   */
  @Test
  public void testContainerMerge() {

    // Simulated data from a reader

    BatchSchema leftSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR)
        .build();
    SingleRowSet left = fixture.rowSetBuilder(leftSchema)
        .add(10, "fred")
        .add(20, "barney")
        .add(30, "wilma")
        .build();

    // Simulated "implicit" coumns: row number and file name

    BatchSchema rightSchema = new SchemaBuilder()
        .add("x", MinorType.SMALLINT)
        .add("y", MinorType.VARCHAR)
        .build();
    SingleRowSet right = fixture.rowSetBuilder(rightSchema)
        .add(1, "foo.txt")
        .add(2, "bar.txt")
        .add(3, "dino.txt")
        .build();

    // The merge batch we expect to see

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addNullable("b", MinorType.VARCHAR)
        .add("x", MinorType.SMALLINT)
        .add("y", MinorType.VARCHAR)
        .build();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .add(10, "fred", 1, "foo.txt")
        .add(20, "barney", 2, "bar.txt")
        .add(30, "wilma", 3, "dino.txt")
        .build();

    // Merge containers without selection vector

    RowSet merged = fixture.wrap(
        left.container().merge(right.container()));

    RowSetComparison comparison = new RowSetComparison(expected);
    comparison.verify(merged);

    // Merge containers via row set facade

    RowSet mergedRs = left.merge(right);
    comparison.verifyAndClearAll(mergedRs);

    // Add a selection vector. Merging is forbidden, in the present code,
    // for batches that have a selection vector.

    SingleRowSet leftIndirect = left.toIndirect();
    try {
      leftIndirect.merge(right);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
    leftIndirect.clear();
    right.clear();
  }
}
