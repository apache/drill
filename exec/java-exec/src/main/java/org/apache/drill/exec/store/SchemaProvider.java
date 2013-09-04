package org.apache.drill.exec.store;

public interface SchemaProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaProvider.class);
  
  public Object getSelectionBaseOnName(String tableName);

}
