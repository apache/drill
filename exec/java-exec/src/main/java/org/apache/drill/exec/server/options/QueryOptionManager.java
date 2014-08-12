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

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.eigenbase.sql.SqlLiteral;

import java.util.Iterator;
import java.util.Map;

public class QueryOptionManager implements OptionManager {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SessionOptionManager.class);

  private Map<String, OptionValue> options = Maps.newConcurrentMap();
  private OptionManager sessionOptions;

  public QueryOptionManager(OptionManager sessionOptions) {
    super();
    this.sessionOptions = sessionOptions;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return Iterables.concat(sessionOptions, options.values()).iterator();
  }

  @Override
  public OptionValue getOption(String name) {
    OptionValue opt = options.get(name);
    if (opt == null && sessionOptions != null) {
      return sessionOptions.getOption(name);
    } else {
      return opt;
    }
  }

  @Override
  public void setOption(OptionValue value) {
    sessionOptions.getAdmin().validate(value);
    setValidatedOption(value);
  }

  @Override
  public void setOption(String name, SqlLiteral literal, OptionValue.OptionType type) {
    OptionValue val = sessionOptions.getAdmin().validate(name, literal);
    val.type = type;
    setValidatedOption(val);
  }

  private void setValidatedOption(OptionValue value) {
    if (value.type == OptionValue.OptionType.QUERY) {
      options.put(value.name, value);
    } else {
      sessionOptions.setOption(value);
    }
  }

  @Override
  public OptionManager.OptionAdmin getAdmin() {
    return sessionOptions.getAdmin();
  }

  @Override
  public OptionManager getSystemManager() {
    return sessionOptions.getSystemManager();
  }

  @Override
  public OptionList getOptionList() {
    OptionList list = new OptionList();
    for (OptionValue o : options.values()) {
      list.add(o);
    }
    return list;
  }

  public OptionList getSessionOptionList() {
    return sessionOptions.getOptionList();
  }

}
