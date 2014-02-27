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

import org.junit.Ignore;
import org.junit.Test;

public class TestTpchQueries extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTpchQueries.class);
  
  @Test
  @Ignore
  public void tpch01() throws Exception{
    testSqlFromFile("queries/tpch/01.sql");
  }
  
  @Test
  @Ignore // DRILL-435
  public void tpch02() throws Exception{
    testSqlFromFile("queries/tpch/02.sql");
  }

  @Test
  @Ignore // DRILL-436
  public void tpch03() throws Exception{
    testSqlFromFile("queries/tpch/03.sql");
  }

  @Test
  @Ignore // DRILL-437
  public void tpch04() throws Exception{
    testSqlFromFile("queries/tpch/04.sql");
  }
  
  @Test 
  @Ignore // DRILL-436
  public void tpch05() throws Exception{
    testSqlFromFile("queries/tpch/05.sql");
  }
  
  @Test  // DRILL-356
  @Ignore
  public void tpch06() throws Exception{
    testSqlFromFile("queries/tpch/06.sql");
  }

  @Test 
  @Ignore // DRILL-439
  public void tpch07() throws Exception{
    testSqlFromFile("queries/tpch/07.sql");
  }

  @Test  
  @Ignore // DRILL-356
  public void tpch08() throws Exception{
    testSqlFromFile("queries/tpch/08.sql");
  }

  @Test  
  @Ignore // DRILL-435
  public void tpch09() throws Exception{
    testSqlFromFile("queries/tpch/09.sql");
  }

  @Test  
  @Ignore // DRILL-356  
  public void tpch10() throws Exception{
    testSqlFromFile("queries/tpch/10.sql");
  }

  @Test  
  @Ignore // DRILL-436
  public void tpch11() throws Exception{
    testSqlFromFile("queries/tpch/11.sql");
  }

  @Test 
  @Ignore // DRILL-403
  public void tpch12() throws Exception{
    testSqlFromFile("queries/tpch/12.sql");
  }

  @Test  
  @Ignore // DRILL-435
  public void tpch13() throws Exception{
    testSqlFromFile("queries/tpch/13.sql");
  }

  @Test  
  @Ignore // DRILL-435
  public void tpch14() throws Exception{
    testSqlFromFile("queries/tpch/14.sql");
  }

  @Test  
  @Ignore // DRILL-438
  public void tpch15() throws Exception{
    testSqlFromFile("queries/tpch/15.sql");
  }

  @Test  
  @Ignore // DRILL-435
  public void tpch16() throws Exception{
    testSqlFromFile("queries/tpch/16.sql");
  }

  @Test
  @Ignore // DRILL-440
  public void tpch17() throws Exception{
    testSqlFromFile("queries/tpch/17.sql");
  }

  @Test 
  @Ignore // DRILL-436
  public void tpch18() throws Exception{
    testSqlFromFile("queries/tpch/18.sql");
  }

  @Test  
  @Ignore // DRILL-436
  public void tpch19() throws Exception{
    testSqlFromFile("queries/tpch/19.sql");
  }

  @Test  
  @Ignore // DRILL-435
  public void tpch20() throws Exception{
    testSqlFromFile("queries/tpch/20.sql");
  }

  @Test  
  @Ignore // DRILL-440
  public void tpch21() throws Exception{
    testSqlFromFile("queries/tpch/21.sql");
  }

  @Test  
  @Ignore // DRILL-441
  public void tpch22() throws Exception{
    testSqlFromFile("queries/tpch/22.sql");
  }

}
