/*
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
package org.apache.drill.exec.store.elasticsearch;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ElasticSearchUtils {

	 // 组装并的条件
  public static String andFilterAtIndex(final String leftFilter,
		  final String String) {
//    Document andQueryFilter = new Document();
//    List<Document> filters = new ArrayList<Document>();
//    filters.add(leftFilter);
//    filters.add(rightFilter);
//    andQueryFilter.put("$and", filters);
	  return String.format("\"{\"and\":{\"filters\":[%s, %s]}}\"");
  }

  public static String orFilterAtIndex(String leftFilter,
		  String rightFilter) {
	  //组装或的条件
//    Document orQueryFilter = new Document();
//    List<Document> filters = new ArrayList<Document>();
//    filters.add(leftFilter);
//    filters.add(rightFilter);
//    orQueryFilter.put("$or", filters);
	  return String.format("\"{\"or\":{\"filters\":[%s, %s]}}\"");
  }

  public static Map<String, List<String>> mergeFilters(
      Map<String, Object> minFilters, Map<String, Object> maxFilters) {
    Map<String, List<String>> filters = Maps.newHashMap();
    // 组装大于 和小于 的条件
    
    for (Entry<String, Object> entry : minFilters.entrySet()) {
      List<String> list = filters.get(entry.getKey());
      if (list == null) {
        list = Lists.newArrayList();
        filters.put(entry.getKey(), list);
      }
//      list.add(new Document(entry.getKey(), new Document("$gte",
//          entry.getValue())));
      
      list.add(String.format("\"{\"range\":{\"%s\":{\"gt\" :%s\"}}}" ,entry.getKey() , entry.getValue()));
    }

    for (Entry<String, Object> entry : maxFilters.entrySet()) {
      List<String> list = filters.get(entry.getKey());
      if (list == null) {
        list = Lists.newArrayList();
        filters.put(entry.getKey(), list);
      }
//      list.add(new Document(entry.getKey(), new Document("$lt", entry
//          .getValue())));
      list.add(String.format("\"{\"range\":{\"%s\":{\"lt\" :%s\"}}}" ,entry.getKey() , entry.getValue()));
    }
    return filters;
  }

}
