package org.apache.drill.jdbc.test;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class TempTypesMockStoragePluginConfig extends StoragePluginConfig {
  private static final Logger logger = getLogger( TempTypesMockStoragePluginConfig.class );
  
  private static int count;

  static {
    logger.info( "??: class initializing" );
  }
  
  TempTypesMockStoragePluginConfig() {
    count++;
    logger.info( "??: TempTypesMockStoragePluginConfig(...) called (count := " + count + ")" );
  }

  public void setFindThisParameterName2( String value  ) {
    logger.info( "??: setFindThisParameterName2( ~\"" + value + "\" ) called" );
  }
  
  public String getFindThisParameterName2() {
    logger.info( "??: getFindThisParameterName2() called" );
    return "hard-coded findThisParameterName2 value";
  }
  
  @Override
  public boolean isEnabled() {
    logger.info( "??: isEnabled() called" );
    return super.isEnabled();
  }

  @Override
  public void setEnabled( boolean enabled ) {
    logger.info( "??: setEnabled( " + enabled + " ) called" );
    super.setEnabled( enabled );
  }

  @Override
  public int hashCode() {
    return 1;
  }
  
  private boolean equals( TempTypesMockStoragePluginConfig o ) {
    return this.isEnabled() == o.isEnabled();
  }

  @Override
  public boolean equals( Object o ) {
    logger.info( "??: equals(...) called" );
    return 
        o instanceof TempTypesMockStoragePluginConfig 
        && equals( (TempTypesMockStoragePluginConfig) o );
  }

}