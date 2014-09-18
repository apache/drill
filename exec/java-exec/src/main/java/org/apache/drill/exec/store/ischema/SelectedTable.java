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
package org.apache.drill.exec.store.ischema;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.ischema.InfoSchemaTable.Catalogs;
import org.apache.drill.exec.store.ischema.InfoSchemaTable.Columns;
import org.apache.drill.exec.store.ischema.InfoSchemaTable.Schemata;
import org.apache.drill.exec.store.ischema.InfoSchemaTable.Tables;
import org.apache.drill.exec.store.ischema.InfoSchemaTable.Views;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

public enum SelectedTable{
  CATALOGS(new Catalogs()),
  SCHEMATA(new Schemata()),
  VIEWS(new Views()),
  COLUMNS(new Columns()),
  TABLES(new Tables());

  private final InfoSchemaTable tableDef;

  private SelectedTable(InfoSchemaTable tableDef) {
    this.tableDef = tableDef;
  }

  public RecordReader getRecordReader(SchemaPlus rootSchema) {
    RecordGenerator recordGenerator = tableDef.getRecordGenerator();
    recordGenerator.scanSchema(rootSchema);
    return recordGenerator.getRecordReader();
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return tableDef.getRowType(typeFactory);
  }
}