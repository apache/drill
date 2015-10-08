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
package org.apache.drill.exec.expr.fn.impl.gis;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestGeometryFunctions extends BaseTestQuery {

  String wktPoint = "POINT (-121.895 37.339)";

  @Test
  public void testGeometryFromTextCreation() throws Exception {

    testBuilder()
    .sqlQuery("select ST_AsText(ST_GeomFromText('" + wktPoint + "')) "
        + "from cp.`/sample-data/CA-cities.csv` limit 1")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(wktPoint)
    .build()
    .run();
  }

  @Test
  public void testGeometryPointCreation() throws Exception {

    testBuilder()
      .sqlQuery("select ST_AsText(ST_Point(-121.895, 37.339)) "
          + "from cp.`/sample-data/CA-cities.csv` limit 1")
      .ordered().baselineColumns("EXPR$0")
      .baselineValues(wktPoint)
      .build()
      .run();
  }

  @Test
  public void testSTWithinQuery() throws Exception {

    testBuilder()
      .sqlQuery("select ST_Within(ST_Point(columns[4], columns[3]),"
          + "ST_GeomFromText('POLYGON((-121.95 37.28, -121.94 37.35, -121.84 37.35, -121.84 37.28, -121.95 37.28))')"
          + ") "
          + "from cp.`/sample-data/CA-cities.csv` where columns[2] = 'San Jose'")
      .ordered().baselineColumns("EXPR$0")
      .baselineValues(true)
      .build()
      .run();


    testBuilder()
    .sqlQuery("select ST_Within(" + "ST_Point(columns[4], columns[3]),"
        + "ST_GeomFromText('POLYGON((-121.95 37.28, -121.94 37.35, -121.84 37.35, -121.84 37.28, -121.95 37.28))')"
        + ") "
        + "from cp.`/sample-data/CA-cities.csv` where columns[2] = 'San Francisco'")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(false)
    .build()
    .run();
  }
}