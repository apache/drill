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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.collections.IteratorUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.QueryClassLoader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.eigenbase.sql.SqlLiteral;

import com.google.common.collect.Maps;

public class SystemOptionManager implements OptionManager {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemOptionManager.class);

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
      PlannerSettings.IDENTIFIER_MAX_LENGTH,
      PlannerSettings.HASH_JOIN_SWAP,
      PlannerSettings.HASH_JOIN_SWAP_MARGIN_FACTOR,
      ExecConstants.CAST_TO_NULLABLE_NUMERIC_OPTION,
      ExecConstants.OUTPUT_FORMAT_VALIDATOR,
      ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR,
      ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE_VALIDATOR,
      ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING_VALIDATOR,
      ExecConstants.PARQUET_VECTOR_FILL_THRESHOLD_VALIDATOR,
      ExecConstants.PARQUET_VECTOR_FILL_CHECK_THRESHOLD_VALIDATOR,
      ExecConstants.PARQUET_RECORD_READER_IMPLEMENTATION_VALIDATOR,
      ExecConstants.JSON_READER_ALL_TEXT_MODE_VALIDATOR,
      ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL_VALIDATOR,
      ExecConstants.MONGO_READER_ALL_TEXT_MODE_VALIDATOR,
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

  public SystemOptionManager(DrillConfig config, PStoreProvider provider) {
    this.provider = provider;
    this.config =  PStoreConfig //
        .newJacksonBuilder(config.getMapper(), OptionValue.class) //
        .name("sys.options") //
        .build();
  }

  public SystemOptionManager init() throws IOException{
    this.options = provider.getStore(config);
    this.admin = new SystemOptionAdmin();
    return this;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    Map<String, OptionValue> buildList = Maps.newHashMap();
    for(OptionValidator v : knownOptions.values()){
      buildList.put(v.getOptionName(), v.getDefault());
    }
    for(Map.Entry<String, OptionValue> v : options){
      OptionValue value = v.getValue();
      buildList.put(value.name, value);
    }
    return buildList.values().iterator();
  }

  @Override
  public OptionValue getOption(String name) {
    // check local space
    OptionValue v = options.get(name);
    if(v != null){
      return v;
    }

    // otherwise, return default.
    OptionValidator validator = knownOptions.get(name);
    if(validator == null){
      return null;
    }else{
      return validator.getDefault();
    }
  }

  @Override
  public void setOption(OptionValue value) {
    assert value.type == OptionType.SYSTEM;
    admin.validate(value);
    setOptionInternal(value);
  }

  private void setOptionInternal(OptionValue value){
    if(!value.equals(knownOptions.get(value.name))){
      options.put(value.name, value);
    }
  }


  @Override
  public void setOption(String name, SqlLiteral literal, OptionType type) {
    assert type == OptionValue.OptionType.SYSTEM;
    OptionValue v = admin.validate(name, literal);
    v.type = type;
    setOptionInternal(v);
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

    public SystemOptionAdmin() {
      for(OptionValidator v : VALIDATORS) {
        knownOptions.put(v.getOptionName(), v);
      }

      for(Entry<String, OptionValue> v : options){
        OptionValue value = v.getValue();
        OptionValidator defaultValidator = knownOptions.get(v.getKey());
        if(defaultValidator == null){
          // deprecated option, delete.
          options.delete(value.name);
          logger.warn("Deleting deprecated option `{}`.", value.name);
        }else if(value.equals(defaultValidator)){
          // option set with default value, remove storage of record.
          options.delete(value.name);
          logger.warn("Deleting option `{}` set to default value.", value.name);
        }

      }

    }

    @Override
    public void registerOptionType(OptionValidator validator) {
      if (null != knownOptions.putIfAbsent(validator.getOptionName(), validator) ) {
        throw new IllegalArgumentException("Only one option is allowed to be registered with name: " + validator.getOptionName());
      }
    }

    @Override
    public void validate(OptionValue v) throws SetOptionException {
      OptionValidator validator = knownOptions.get(v.name);
      if (validator == null) {
        throw new SetOptionException("Unknown option " + v.name);
      }
      validator.validate(v);
    }

    @Override
    public OptionValue validate(String name, SqlLiteral value) throws SetOptionException {
      OptionValidator validator = knownOptions.get(name);
      if (validator == null) {
        throw new SetOptionException("Unknown option: " + name);
      }
      return validator.validate(value);
    }

  }

}
