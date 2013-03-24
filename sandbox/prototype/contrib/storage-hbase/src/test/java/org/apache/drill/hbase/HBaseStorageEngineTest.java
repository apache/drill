/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.hbase;

import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.hbase.table.HBaseTableScanner;
import org.apache.drill.hbase.table.HBaseTableScannerRecordReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.NavigableMap;

import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.primitives.UnsignedBytes.lexicographicalComparator;
import static junit.framework.Assert.*;
import static org.apache.drill.hbase.HbaseUtils.nameToBytes;
import static org.easymock.EasyMock.*;

public class HBaseStorageEngineTest {

  private NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultWithSingleVersion;
  private NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultWithTwoVersions;

  @Before
  public void setUp() {
    resultWithSingleVersion = newTreeMap(lexicographicalComparator());
    resultWithTwoVersions = newTreeMap(lexicographicalComparator());

    NavigableMap<byte[], NavigableMap<Long, byte[]>> familyA = newTreeMap(lexicographicalComparator());
    NavigableMap<byte[], NavigableMap<Long, byte[]>> familyB = newTreeMap(lexicographicalComparator());

    NavigableMap<Long, byte[]> columnA = newTreeMap();
    NavigableMap<Long, byte[]> columnB = newTreeMap();
    NavigableMap<Long, byte[]> columnC = newTreeMap();
    NavigableMap<Long, byte[]> columnD = newTreeMap();

    columnA.put(0L, nameToBytes("columnA"));
    columnB.put(0L, nameToBytes("columnB"));
    columnC.put(0L, nameToBytes("columnC1"));
    columnC.put(1L, nameToBytes("columnC2"));
    columnD.put(0L, nameToBytes("columnD1"));
    columnD.put(1L, nameToBytes("columnD2"));

    familyA.put(nameToBytes("columnA"), columnA);
    familyA.put(nameToBytes("columnB"), columnB);
    familyB.put(nameToBytes("columnC"), columnC);
    familyB.put(nameToBytes("columnD"), columnD);

    resultWithSingleVersion.put(nameToBytes("familyA"), familyA);
    resultWithTwoVersions.put(nameToBytes("familyA"), familyA);
    resultWithTwoVersions.put(nameToBytes("familyB"), familyB);
  }

  @Test
  public void testReadHBaseDataValuesWithSingleVersion() throws IOException {
    Result resultA = createMock(Result.class);
    expect(resultA.getRow()).andReturn(nameToBytes("rowA"));
    expect(resultA.getMap()).andReturn(resultWithSingleVersion);

    ResultScanner scanner = createMock(ResultScanner.class);
    expect(scanner.iterator()).andReturn(ImmutableSet.of(resultA).iterator());

    HBaseTableScanner table = createMock(HBaseTableScanner.class);
    expect(table.newScanner()).andReturn(scanner);

    replay(resultA, scanner, table);

    HBaseTableScannerRecordReader reader = new HBaseTableScannerRecordReader(table, null);

    RecordIterator iterator = reader.getIterator();

    RecordPointer pointerA = iterator.getRecordPointer();

    assertNotNull(pointerA);
    iterator.next();
    assertNull(iterator.getRecordPointer());

    DataValue value = pointerA.getField(new SchemaPath("row"));
    assertNotNull(value);
  }

  @Test
  public void testEnginesWithTheSameNameAreEqual() {
    DrillConfig config = DrillConfig.create();
    RSERegistry rses = new RSERegistry(config);
    StorageEngineConfig hconfig =  new HBaseStorageEngine.HBaseStorageEngineConfig("hbase");
    HBaseStorageEngine engine = (HBaseStorageEngine) rses.getEngine(hconfig);
    HBaseStorageEngine engine2 = (HBaseStorageEngine) rses.getEngine(hconfig);
    assertEquals(engine, engine2);
  }

}
