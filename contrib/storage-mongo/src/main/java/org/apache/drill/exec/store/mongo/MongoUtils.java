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
package org.apache.drill.exec.store.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.LazyBSONCallback;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.LazyWriteableDBObject;

public class MongoUtils {

  public static BasicDBObject andFilterAtIndex(BasicDBObject leftFilter,
      BasicDBObject rightFilter) {
    BasicDBObject andQueryFilter = new BasicDBObject();
    List<BasicDBObject> filters = new ArrayList<BasicDBObject>();
    filters.add(leftFilter);
    filters.add(rightFilter);
    andQueryFilter.put("$and", filters);
    return andQueryFilter;
  }

  public static BasicDBObject orFilterAtIndex(BasicDBObject leftFilter,
      BasicDBObject rightFilter) {
    BasicDBObject orQueryFilter = new BasicDBObject();
    List<BasicDBObject> filters = new ArrayList<BasicDBObject>();
    filters.add(leftFilter);
    filters.add(rightFilter);
    orQueryFilter.put("$or", filters);
    return orQueryFilter;
  }

  public static BasicDBObject deserializeFilter(byte[] filterBytes) {
    DBObject dbo = new LazyWriteableDBObject(filterBytes,
        new LazyBSONCallback());
    BasicDBObject result = new BasicDBObject();
    result.putAll(dbo);
    return result;
  }

  public static Map<String, List<BasicDBObject>> mergeFilters(
      Map<String, Object> minFilters, Map<String, Object> maxFilters) {
    Map<String, List<BasicDBObject>> filters = Maps.newHashMap();

    for (Entry<String, Object> entry : minFilters.entrySet()) {
      List<BasicDBObject> list = filters.get(entry.getKey());
      if (list == null) {
        list = Lists.newArrayList();
        filters.put(entry.getKey(), list);
      }
      list.add(new BasicDBObject(entry.getKey(), new BasicDBObject("$gte",
          entry.getValue())));
    }

    for (Entry<String, Object> entry : maxFilters.entrySet()) {
      List<BasicDBObject> list = filters.get(entry.getKey());
      if (list == null) {
        list = Lists.newArrayList();
        filters.put(entry.getKey(), list);
      }
      list.add(new BasicDBObject(entry.getKey(), new BasicDBObject("$lt", entry
          .getValue())));
    }
    return filters;
  }

}
