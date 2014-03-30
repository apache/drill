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
package org.apache.drill;

import java.io.IOException;
import java.net.URL;

import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.client.QuerySubmitter;
import org.apache.drill.exec.store.ResourceInputStream;
import org.junit.Rule;
import org.junit.rules.TestRule;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);
  
  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(20000);
  
  protected void test(String sql) throws Exception{
    boolean good = false;
    sql = sql.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    
    try{
      QuerySubmitter s = new QuerySubmitter();
      s.submitQuery(null, sql, "sql", null, true, 1, "tsv");
      good = true;
    }finally{
      if(!good) Thread.sleep(2000);
    }
  }
  
  protected void testLogical(String logical) throws Exception{
    boolean good = false;
    logical = logical.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    
    try{
      QuerySubmitter s = new QuerySubmitter();
      s.submitQuery(null, logical, "logical", null, true, 1, "tsv");
      good = true;
    }finally{
      if(!good) Thread.sleep(2000);
    }
  }
  
  protected void testPhysical(String physical) throws Exception{
    boolean good = false;
    physical = physical.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    
    try{
      QuerySubmitter s = new QuerySubmitter();
      s.submitQuery(null, physical, "physical", null, true, 1, "tsv");
      good = true;
    }finally{
      if(!good) Thread.sleep(2000);
    }
  }
  
  protected void testPhysicalFromFile(String file) throws Exception{
    testPhysical(getFile(file));
  }
  protected void testLogicalFromFile(String file) throws Exception{
    testLogical(getFile(file));
  }
  protected void testSqlFromFile(String file) throws Exception{
    test(getFile(file));
  }
  
  protected String getFile(String resource) throws IOException{
    URL url = Resources.getResource(resource);
    if(url == null){
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }
}
