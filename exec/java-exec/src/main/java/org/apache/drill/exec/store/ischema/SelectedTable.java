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