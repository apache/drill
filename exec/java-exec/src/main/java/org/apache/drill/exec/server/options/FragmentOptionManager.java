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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.eigenbase.sql.SqlLiteral;

import java.util.Iterator;
import java.util.Map;

public class FragmentOptionManager implements OptionManager {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentOptionManager.class);

  ImmutableMap<String, OptionValue> options;
  OptionManager systemOptions;

  public FragmentOptionManager(OptionManager systemOptions, OptionList options) {
    Map<String, OptionValue> tmp = Maps.newHashMap();
    for(OptionValue v : options){
      tmp.put(v.name, v);
    }
    this.options = ImmutableMap.copyOf(tmp);
    this.systemOptions = systemOptions;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return Iterables.concat(systemOptions, options.values()).iterator();
  }

  @Override
  public OptionValue getOption(String name) {
    OptionValue value = options.get(name);
    if (value == null && systemOptions != null) {
      value = systemOptions.getOption(name);
    }
    return value;
  }

  @Override
  public void setOption(OptionValue value) throws SetOptionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setOption(String name, SqlLiteral literal, OptionValue.OptionType type) throws SetOptionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public OptionAdmin getAdmin() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OptionManager getSystemManager() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OptionList getOptionList() {
    throw new UnsupportedOperationException();
  }

}
