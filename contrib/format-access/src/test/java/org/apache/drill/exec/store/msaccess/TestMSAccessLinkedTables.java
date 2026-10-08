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

package org.apache.drill.exec.store.msaccess;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import org.apache.drill.categories.RowSetTest;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

/**
 * Linked tables with {@link MSAccessFormatPlugin#ALLOW_LINKED_DATABASES} enabled by the administrator.
 * The default (disabled) behaviour is covered in {@link TestMSAccessReader}.
 */
@Category(RowSetTest.class)
public class TestMSAccessLinkedTables extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher)
        .configProperty(MSAccessFormatPlugin.ALLOW_LINKED_DATABASES, true));
  }

  @Test
  public void testLinkedTableResolvedWhenAllowed() throws Exception {
    File target = new File(dirTestWatcher.getRootDir(), "linked_target.mdb");
    try (Database db = DatabaseBuilder.create(Database.FileFormat.V2003, target)) {
      Table table = new TableBuilder("Table1")
          .addColumn(new ColumnBuilder("name", DataType.TEXT))
          .toTable(db);
      table.addRow("from the linked database");
    }

    File linker = new File(dirTestWatcher.getRootDir(), "linker.mdb");
    try (Database db = DatabaseBuilder.create(Database.FileFormat.V2003, linker)) {
      db.createLinkedTable("Linked", target.getAbsolutePath(), "Table1");
    }

    String sql = "SELECT * FROM table(dfs.`" + linker.getName() + "` (type=> 'msaccess', tableName => 'Linked'))";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("name", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("from the linked database")
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
