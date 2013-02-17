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
package org.apache.drill.common.config;

import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.StorageEngineConfigBase;
import org.apache.drill.common.logical.data.LogicalOperatorBase;
import org.apache.drill.common.util.PathScanner;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public final class DrillConfig extends NestedConfig{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConfig.class);
  private final ObjectMapper mapper;
  
  @SuppressWarnings("unchecked")
  private volatile List<Queue<Object>> sinkQueues = new CopyOnWriteArrayList<Queue<Object>>(new Queue[1]);
  
  private DrillConfig(Config config) {
    super(config);
    mapper = new ObjectMapper();
    SimpleModule deserModule = new SimpleModule("LogicalExpressionDeserializationModule").addDeserializer(LogicalExpression.class, new LogicalExpression.De(this));
    mapper.registerModule(deserModule);
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(Feature.ALLOW_COMMENTS, true);
    mapper.registerSubtypes(LogicalOperatorBase.getSubTypes(this));
    mapper.registerSubtypes(StorageEngineConfigBase.getSubTypes(this));
    
  };
  
  /**
   * <p>
   * DrillConfig loads up Drill configuration information. It does this utilizing a combination of classpath scanning
   * and Configuration fallbacks provided by the TypeSafe configuration library. The order of precedence is as
   * follows:
   * </p>
   * <p>
   * Configuration values are retrieved as follows:
   * <ul>
   * <li>Check a single copy of "drill-override.conf". If multiple copies are on the classpath, behavior is
   * indeterminate.</li>
   * <li>Check all copies of "drill-module.conf". Loading order is indeterminate.</li>
   * <li>Check a single copy of "drill-default.conf". If multiple copies are on the classpath, behavior is
   * indeterminate.</li>
   * </ul>
   * 
   * </p>
   * * @return A merged Config object.
   */
  public static DrillConfig create() {
    // first we load defaults.
    Config fallback = ConfigFactory.load(CommonConstants.CONFIG_DEFAULT);
    Collection<URL> urls = PathScanner.getConfigURLs();
    logger.debug("Loading configs at the following URLs {}", urls);
    for (URL url : urls) {
      fallback = ConfigFactory.parseURL(url).withFallback(fallback);
    }

    Config c = ConfigFactory.load(CommonConstants.CONFIG_OVERRIDE).withFallback(fallback).resolve();
    return new DrillConfig(c);
  }
  
  public void setSinkQueues(int number, Queue<Object> queue){
    sinkQueues.set(number, queue);
  }
  
  public Queue<Object> getQueue(int number){
    if(sinkQueues.size() <= number || number < 0 || sinkQueues == null) throw new IllegalArgumentException(String.format("Queue %d is not available.", number));
    return sinkQueues.get(number);
  }
  
  public ObjectMapper getMapper(){
    return mapper;
  }
}
