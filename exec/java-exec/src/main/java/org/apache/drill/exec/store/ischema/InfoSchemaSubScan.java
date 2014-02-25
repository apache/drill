package org.apache.drill.exec.store.ischema;

import org.apache.drill.exec.physical.base.AbstractSubScan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InfoSchemaSubScan extends AbstractSubScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaSubScan.class);
  

  private final SelectedTable table;
  
  @JsonCreator
  public InfoSchemaSubScan(@JsonProperty("table") SelectedTable table) {
    this.table = table;
  }

  public SelectedTable getTable() {
    return table;
  }
  
  
}
