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

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.server.options.OptionValue.OptionType;

/**
 * {@link OptionManager} that holds options within {@link org.apache.drill.exec.ops.QueryContext}.
 */
public class QueryOptionManager extends InMemoryOptionManager {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryOptionManager.class);

  public QueryOptionManager(OptionManager sessionOptions) {
    super(sessionOptions, CaseInsensitiveMap.<OptionValue>newHashMap());
  }

  @Override
  public OptionList getOptionList() {
    OptionList list = super.getOptionList();
    list.merge(fallback.getOptionList());
    return list;
  }

  @Override
  boolean supportsOptionType(OptionType type) {
    return type == OptionType.QUERY;
  }
}
