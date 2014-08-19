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
package org.apache.drill.exec.server.options;

import com.google.common.collect.Maps;
import org.apache.commons.collections.IteratorUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.DistributedCache.CacheConfig;
import org.apache.drill.exec.compile.QueryClassLoader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.eigenbase.sql.SqlLiteral;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class SystemOptionManager implements OptionManager{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemOptionManager.class);

  public static final CacheConfig<String, OptionValue> OPTION_CACHE = CacheConfig //
      .newBuilder(OptionValue.class) //
      .name("sys.options") //
      .jackson()
      .build();

  private final OptionValidator[] VALIDATORS = {
      PlannerSettings.EXCHANGE,
      PlannerSettings.HASHAGG,
      PlannerSettings.STREAMAGG,
      PlannerSettings.HASHJOIN,
      PlannerSettings.MERGEJOIN,
      PlannerSettings.MULTIPHASE,
      PlannerSettings.BROADCAST,
      PlannerSettings.BROADCAST_THRESHOLD,
      PlannerSettings.JOIN_ROW_COUNT_ESTIMATE_FACTOR,
      PlannerSettings.PRODUCER_CONSUMER,
      PlannerSettings.PRODUCER_CONSUMER_QUEUE_SIZE,
      PlannerSettings.HASH_SINGLE_KEY,
      ExecConstants.OUTPUT_FORMAT_VALIDATOR,
      ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR,
      ExecConstants.PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR,
      ExecConstants.SLICE_TARGET_OPTION,
      ExecConstants.AFFINITY_FACTOR,
      ExecConstants.MAX_WIDTH_GLOBAL,
      ExecConstants.MAX_WIDTH_PER_NODE,
      ExecConstants.ENABLE_QUEUE,
      ExecConstants.LARGE_QUEUE_SIZE,
      ExecConstants.QUEUE_THRESHOLD_SIZE,
      ExecConstants.QUEUE_TIMEOUT,
      ExecConstants.SMALL_QUEUE_SIZE,
      ExecConstants.MIN_HASH_TABLE_SIZE,
      ExecConstants.MAX_HASH_TABLE_SIZE,
      ExecConstants.ENABLE_MEMORY_ESTIMATION,
      ExecConstants.MAX_QUERY_MEMORY_PER_NODE,
      ExecConstants.NON_BLOCKING_OPERATORS_MEMORY,
      ExecConstants.HASH_JOIN_TABLE_FACTOR,
      ExecConstants.HASH_AGG_TABLE_FACTOR,
      ExecConstants.AVERAGE_FIELD_WIDTH,
      QueryClassLoader.JAVA_COMPILER_VALIDATOR,
      QueryClassLoader.JAVA_COMPILER_JANINO_MAXSIZE,
      QueryClassLoader.JAVA_COMPILER_DEBUG,
      ExecConstants.ENABLE_VERBOSE_ERRORS
  };

  public final PStoreConfig<OptionValue> config;

  private PStore<OptionValue> options;
  private SystemOptionAdmin admin;
  private final ConcurrentMap<String, OptionValidator> knownOptions = Maps.newConcurrentMap();
  private final PStoreProvider provider;

  public SystemOptionManager(DrillConfig config, PStoreProvider provider){
    this.provider = provider;
    this.config =  PStoreConfig //
        .newJacksonBuilder(config.getMapper(), OptionValue.class) //
        .name("sys.options") //
        .build();
  }

  public void init() throws IOException{
    this.options = provider.getPStore(config);
    this.admin = new SystemOptionAdmin();
  }

  private class Iter implements Iterator<OptionValue>{
    private Iterator<Map.Entry<String, OptionValue>> inner;

    public Iter(Iterator<Map.Entry<String, OptionValue>> inner){
      this.inner = inner;
    }

    @Override
    public boolean hasNext() {
      return inner.hasNext();
    }

    @Override
    public OptionValue next() {
      return inner.next().getValue();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }
  @Override
  public Iterator<OptionValue> iterator() {
    return new Iter(options.iterator());
  }

  @Override
  public OptionValue getOption(String name) {
    return options.get(name);
  }

  @Override
  public void setOption(OptionValue value) {
    assert value.type == OptionType.SYSTEM;
    admin.validate(value);
    options.put(value.name, value);
  }

  @Override
  public void setOption(String name, SqlLiteral literal, OptionType type) {
    assert type == OptionValue.OptionType.SYSTEM;
    OptionValue v = admin.validate(name, literal);
    v.type = type;
    options.put(name, v);
  }

  @Override
  public OptionList getOptionList() {
    return (OptionList) IteratorUtils.toList(iterator());
  }

  @Override
  public OptionManager getSystemManager() {
    return this;
  }

  @Override
  public OptionAdmin getAdmin() {
    return admin;
  }

  private class SystemOptionAdmin implements OptionAdmin{

    public SystemOptionAdmin(){
      for(OptionValidator v : VALIDATORS){
        knownOptions.put(v.getOptionName(), v);
        options.putIfAbsent(v.getOptionName(), v.getDefault());
      }
    }


    @Override
    public void registerOptionType(OptionValidator validator) {
      if(null != knownOptions.putIfAbsent(validator.getOptionName(), validator) ){
        throw new IllegalArgumentException("Only one option is allowed to be registered with name: " + validator.getOptionName());
      }
    }

    @Override
    public void validate(OptionValue v) throws SetOptionException {
      OptionValidator validator = knownOptions.get(v.name);
      if(validator == null) throw new SetOptionException("Unknown option " + v.name);
      validator.validate(v);
    }

    @Override
    public OptionValue validate(String name, SqlLiteral value) throws SetOptionException {
      OptionValidator validator = knownOptions.get(name);
      if(validator == null) throw new SetOptionException("Unknown option: " + name);
      return validator.validate(value);
    }



  }


}
