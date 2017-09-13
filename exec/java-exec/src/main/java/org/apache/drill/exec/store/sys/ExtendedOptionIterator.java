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
package org.apache.drill.exec.store.sys;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import com.google.common.collect.Lists;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.server.options.OptionValue.OptionScope;

/*
 * Extends the original Option iterator. The idea is to hide the implementation details and present the
 * user with the rows which have values set at the top level of hierarchy and exclude the values set
 * at lower levels. This is done by examining the scope and the precedence order of scope is session - system - default.
 * All the values are represented as String instead of having multiple
 * columns and the data type is provided as kind to preserve type information about the option.
 * The query output is as follows  -
 *               name,kind,type,val,optionScope
 *        planner.slice_target,BIGINT,SESSION,20,SESSION
 *        planner.width.max_per_node,BIGINT,SYSTEM,0,BOOT
 *        planner.affinity_factor,FLOAT,SYSTEM,1.7,SYSTEM
 *  In the above example, the query output contains single row for each option
 *  and we can infer that slice target is set at the session level and max width
 *  per node is set at the BOOT level and affinity factor is set at the SYSTEM level.
 *  The options set in the top level of hierarchy always takes precedence and they are returned
 *  in the query output. For example if the option is set at both SESSION level and
 *  SYSTEM level the value set at SESSION level takes precedence and query output has
 *  only the value set at SESSION level.
 */
public class ExtendedOptionIterator implements Iterator<Object> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OptionIterator.class);

  private final OptionManager fragmentOptions;
  private final Iterator<OptionValue> mergedOptions;

  public ExtendedOptionIterator(FragmentContext context, boolean internal) {
    fragmentOptions = context.getOptions();
    final Iterator<OptionValue> optionList;

    if (!internal) {
      mergedOptions = sortOptions(fragmentOptions.getPublicOptionList().iterator());
    } else {
      mergedOptions = sortOptions(fragmentOptions.getInternalOptionList().iterator());
    }
  }
   /* *
    * Remove the redundant rows for the same option based on the scope and return
    * the value that takes precedence over others. For example the option set in session
    * scope takes precedence over system and boot and etc.,
    */
  public Iterator<OptionValue> sortOptions(Iterator<OptionValue> options) {
    List<OptionValue> optionslist = Lists.newArrayList(options);
    HashMap<String, OptionValue> optionsmap = new HashMap<>();
    final Map<OptionScope, Integer> preference = new HashMap<OptionScope, Integer>() {{
      put(OptionScope.SESSION, 0);
      put(OptionScope.SYSTEM, 1);
      put(OptionScope.BOOT, 2);
    }};

    for (OptionValue option : optionslist) {
      if (optionsmap.containsKey(option.getName())) {

        if (preference.get(option.scope) < preference.get(optionsmap.get(option.getName()).scope)) {
          optionsmap.put(option.getName(), option);
        }

      } else {
        optionsmap.put(option.getName(), option);
      }
    }
    optionslist.clear();
    for (String name : optionsmap.keySet()) {
      optionslist.add(optionsmap.get(name));
    }

    Collections.sort(optionslist, new Comparator<OptionValue>() {
      @Override
      public int compare(OptionValue v1, OptionValue v2) {
        return v1.name.compareTo(v2.name);
      }
    });

    return optionslist.iterator();
  }

  @Override
  public boolean hasNext() {
    return mergedOptions.hasNext();
  }

  @Override
  public ExtendedOptionValueWrapper next() {
    final OptionValue value = mergedOptions.next();
    final HashMap<OptionValue.Kind,String> typeMapping = new HashMap() {{
      put(Kind.STRING,"VARCHAR");
      put(Kind.DOUBLE,"FLOAT");
      put(Kind.LONG,"BIGINT");
      put(Kind.BOOLEAN,"BIT");

    }};
    return new ExtendedOptionValueWrapper(value.name, typeMapping.get(value.kind), value.accessibleScopes,value.getValue().toString(), value.scope);
  }

  public enum Status {
    BOOT, DEFAULT, CHANGED
  }

  /**
   * Wrapper class for Extended Option Value
   */
  public static class ExtendedOptionValueWrapper {

    public final String name;
    public final String kind;
    public final OptionValue.AccessibleScopes accessibleScopes;
    public final String val;
    public final OptionScope optionScope;


    public ExtendedOptionValueWrapper(final String name, final String kind, final OptionValue.AccessibleScopes type, final String value, final OptionScope scope) {
      this.name = name;
      this.kind = kind;
      this.accessibleScopes = type;
      this.val = value;
      this.optionScope = scope;
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}


