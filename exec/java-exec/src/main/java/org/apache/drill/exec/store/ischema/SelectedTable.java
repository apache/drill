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

import org.apache.drill.exec.store.ischema.InfoSchemaTable.Catalogs;
import org.apache.drill.exec.store.ischema.InfoSchemaTable.Columns;
import org.apache.drill.exec.store.ischema.InfoSchemaTable.Schemata;
import org.apache.drill.exec.store.ischema.InfoSchemaTable.Tables;
import org.apache.drill.exec.store.ischema.InfoSchemaTable.Views;
import org.apache.drill.exec.store.ischema.OptiqProvider.OptiqScanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

public enum SelectedTable{
  CATALOGS(new Catalogs(), new ScannerFactory(){public OptiqScanner get(SchemaPlus root) {return new OptiqProvider.Catalogs(root);}} ), //
  SCHEMATA(new Schemata(), new ScannerFactory(){public OptiqScanner get(SchemaPlus root) {return new OptiqProvider.Schemata(root);}} ), //
  VIEWS(new Views(), new ScannerFactory(){public OptiqScanner get(SchemaPlus root) {return new OptiqProvider.Views(root);}} ), //
  COLUMNS(new Columns(), new ScannerFactory(){public OptiqScanner get(SchemaPlus root) {return new OptiqProvider.Columns(root);}} ), //
  TABLES(new Tables(), new ScannerFactory(){public OptiqScanner get(SchemaPlus root) {return new OptiqProvider.Tables(root);}} ); //
  
  private final FixedTable tableDef;
  private final ScannerFactory providerFactory;
  
  private SelectedTable(FixedTable tableDef, ScannerFactory providerFactory) {
    this.tableDef = tableDef;
    this.providerFactory = providerFactory;
  }
  
  public OptiqScanner getProvider(SchemaPlus root){
    return providerFactory.get(root);
  }
  
  private interface ScannerFactory{
    public OptiqScanner get(SchemaPlus root);
  }
  
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return tableDef.getRowType(typeFactory);
  }
  
  public FixedTable getFixedTable(){
    return tableDef;
  }
}