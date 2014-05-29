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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.eigenbase.sql.SqlLiteral;

import com.google.common.collect.Maps;

public class SystemOptionManager implements OptionManager{

  private final OptionValidator[] VALIDATORS = {
      PlannerSettings.EXCHANGE, 
      PlannerSettings.HASHAGG,
      PlannerSettings.STREAMAGG,
      PlannerSettings.HASHJOIN,
      PlannerSettings.MERGEJOIN, 
      PlannerSettings.MULTIPHASE,
      PlannerSettings.BROADCAST,
      PlannerSettings.BROADCAST_THRESHOLD,
      ExecConstants.OUTPUT_FORMAT_VALIDATOR,
      ExecConstants.PARQUET_BLOCK_SIZE_VALIDATOR
  };

  private DistributedMap<OptionValue> options;
  private SystemOptionAdmin admin;
  private final ConcurrentMap<String, OptionValidator> knownOptions = Maps.newConcurrentMap();
  private DistributedCache cache;

  public SystemOptionManager(DistributedCache cache){
    this.cache = cache;
  }

  public void init(){
    this.options = cache.getNamedMap("system.options", OptionValue.class);
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
    admin.validate(value);
    assert value.type == OptionType.SYSTEM;
    options.put(value.name, value);
  }

  @Override
  public void setOption(String name, SqlLiteral literal) {
    OptionValue v = admin.validate(name, literal);
    v.type = OptionValue.OptionType.SYSTEM;
    options.put(name, v);
  }

  @Override
  public OptionList getSessionOptionList() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OptionManager getSystemManager() {
    return this;
  }

  @Override
  public OptionAdmin getAdmin() {
    return admin;
  }

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemOptionManager.class);


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
      if(validator == null) throw new SetOptionException("Unknown option " + name);
      return validator.validate(value);
    }



  }


}
