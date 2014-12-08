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

public abstract class FallbackOptionManager implements OptionManager{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FallbackOptionManager.class);

  protected OptionManager fallback;

  public FallbackOptionManager(OptionManager fallback) {
    super();
    this.fallback = fallback;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return Iterables.concat(fallback, optionIterable()).iterator();
  }

  @Override
  public OptionValue getOption(String name) {
    OptionValue opt = getLocalOption(name);
    if(opt == null && fallback != null){
      return fallback.getOption(name);
    }else{
      return opt;
    }
  }

  abstract Iterable<OptionValue> optionIterable();
  abstract OptionValue getLocalOption(String name);
  abstract boolean setLocalOption(OptionValue value);

  @Override
  public void setOption(OptionValue value) {
    fallback.getAdmin().validate(value);
    setValidatedOption(value);
  }

  @Override
  public void setOption(String name, SqlLiteral literal, OptionType type) {
    OptionValue val = getAdmin().validate(name, literal);
    val.type = type;
    setValidatedOption(val);
  }

  private void setValidatedOption(OptionValue value) {
    if (!setLocalOption(value)) {
      fallback.setOption(value);
    }else{
      setLocalOption(value);
    }
  }


  @Override
  public OptionAdmin getAdmin() {
    return fallback.getAdmin();
  }

  @Override
  public OptionManager getSystemManager() {
    return fallback.getSystemManager();
  }

  @Override
  public OptionList getOptionList() {
    OptionList list = new OptionList();
    for (OptionValue o : optionIterable()) {
      list.add(o);
    }
    return list;
  }

}
