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

import org.bson.LazyBSONCallback;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.LazyWriteableDBObject;

public class MongoUtils {
  
//  private final static ObjectMapper mapper = new ObjectMapper(MongoBsonFactory.createFactory());
//
//
//  static {
//    mapper.setVisibilityChecker(VisibilityChecker.Std.defaultInstance().withFieldVisibility(
//            JsonAutoDetect.Visibility.ANY));
//}
  
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
    DBObject dbo = new LazyWriteableDBObject(filterBytes, new LazyBSONCallback());
    BasicDBObject result = new BasicDBObject();
    result.putAll(dbo);
    return result;
  }

}
