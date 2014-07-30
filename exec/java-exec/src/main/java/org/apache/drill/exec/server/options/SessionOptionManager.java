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

import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.eigenbase.sql.SqlLiteral;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public class SessionOptionManager implements OptionManager{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SessionOptionManager.class);

  private Map<String, OptionValue> options = Maps.newConcurrentMap();
  private OptionManager systemOptions;

  public SessionOptionManager(OptionManager systemOptions) {
    super();
    this.systemOptions = systemOptions;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return Iterables.concat(systemOptions, options.values()).iterator();
  }

  @Override
  public OptionValue getOption(String name) {
    OptionValue opt = options.get(name);
    if(opt == null && systemOptions != null){
      return systemOptions.getOption(name);
    }else{
      return opt;
    }
  }

  @Override
  public void setOption(OptionValue value) {
    systemOptions.getAdmin().validate(value);
    setValidatedOption(value);
  }

  @Override
  public OptionList getSessionOptionList() {
    OptionList list = new OptionList();
    for(OptionValue o : options.values()){
      list.add(o);
    }
    return list;
  }

  private void setValidatedOption(OptionValue value){
    if(value.type == OptionType.SYSTEM){
      systemOptions.setOption(value);
    }else{
      options.put(value.name, value);
    }
  }

  @Override
  public void setOption(String name, SqlLiteral literal) {
    OptionValue val = systemOptions.getAdmin().validate(name,  literal);
    val.type = OptionValue.OptionType.SESSION;
    setValidatedOption(val);
  }

  @Override
  public OptionAdmin getAdmin() {
    return systemOptions.getAdmin();
  }

  @Override
  public OptionManager getSystemManager() {
    return systemOptions;
  }

}
