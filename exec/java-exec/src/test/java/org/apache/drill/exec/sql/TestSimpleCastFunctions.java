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
package org.apache.drill.exec.sql;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestSimpleCastFunctions extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimpleCastFunctions.class);

  @Test
  public void castFromBoolean() throws Exception {
    test("select cast(false as varchar(5)), cast(true as varchar(4)), cast((1 < 5) as varchar(4)) from sys.options limit 1;");
  }

  @Test
  public void castToBoolean() throws Exception {
    test("select cast('false' as boolean), cast('true' as boolean) from sys.options limit 1;");
  }
}
