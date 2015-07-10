/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.drill.jdbc.test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

public class TempTypesMockStoragePlugin implements StoragePlugin {
  private static final Logger logger = getLogger( TempTypesMockStoragePlugin.class );

  private static int count;

  private final TempTypesMockStoragePluginConfig config;

  public TempTypesMockStoragePlugin(TempTypesMockStoragePluginConfig config,
                                    DrillbitContext context,
                                    String whatKindOfName) {
    count++;
    logger.info( "??: TempTypesMockStoragePlugin(...) called (count := " + count + ")" );
    System.err.println( "???: config = " + config );
    System.err.println( "???: context = " + context );
    System.err.println( "???: whatKindOfName = " + whatKindOfName );
    //??????throw new RuntimeException( "??? NIY" );
    this.config = config;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent)
      throws IOException {
    System.err.println( "registerSchemas(...): schemaConfig = " + schemaConfig );
    System.err.println( "registerSchemas(...): parent = " + parent );
    logger.info( "??: registerSchemas(...) called - ?? CURRRENTLY DOING NOTHING" );
  }

  @Override
  public boolean supportsRead() {
    logger.info( "??: xxx(...) called" );
    throw new RuntimeException( "??? NIY3" );
  }

  @Override
  public boolean supportsWrite() {
    logger.info( "??: xxx(...) called" );
    throw new RuntimeException( "??? NIY4" );
  }

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    logger.info( "??: getOptimizerRules() called" );
    return null; //????
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName,
                                           JSONOptions selection)
      throws IOException {
    logger.info( "??: xxx(...) called" );
    throw new RuntimeException( "??? NI6Y" );
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName,
                                           JSONOptions selection,
                                           List<SchemaPath> columns)
      throws IOException {
    logger.info( "??: xxx(...) called" );
    throw new RuntimeException( "??? NIY7" );
  }

  @Override
  public StoragePluginConfig getConfig() {
    logger.info( "??: getConfig() called" );
    return config;
  }

}