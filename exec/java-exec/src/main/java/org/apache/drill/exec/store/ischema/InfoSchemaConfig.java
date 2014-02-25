package org.apache.drill.exec.store.ischema;

import org.apache.drill.common.logical.StoragePluginConfig;

public class InfoSchemaConfig implements StoragePluginConfig{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaConfig.class);
  
  @Override
  public int hashCode(){
    return 1;
  }
  
  @Override
  public boolean equals(Object o){
    return o instanceof InfoSchemaConfig;
  }
  
}
