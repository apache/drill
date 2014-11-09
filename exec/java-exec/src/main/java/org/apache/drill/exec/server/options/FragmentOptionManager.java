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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class FragmentOptionManager extends InMemoryOptionManager {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentOptionManager.class);

  public FragmentOptionManager(OptionManager systemOptions, OptionList options) {
    super(systemOptions, getMapFromOptionList(options));
  }

  private static Map<String, OptionValue> getMapFromOptionList(OptionList options){
    Map<String, OptionValue> tmp = Maps.newHashMap();
    for(OptionValue v : options){
      tmp.put(v.name, v);
    }
    return ImmutableMap.copyOf(tmp);
  }

  @Override
  boolean supportsOption(OptionValue value) {
    throw new UnsupportedOperationException();
  }



}
