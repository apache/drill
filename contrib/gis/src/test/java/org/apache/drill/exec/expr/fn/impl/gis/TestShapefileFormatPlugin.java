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

public class TestShapefileFormatPlugin extends BaseTestQuery {

  @Test
  public void testRowCount() throws Exception {

    testBuilder()
    .sqlQuery("select count(*) "
        + "from cp.`/sample-data/CA-cities.shp`")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(5727L)
    .build()
    .run();
  }

  @Test
  public void testShpQuery() throws Exception {

    testBuilder()
    .sqlQuery("select gid, srid, shapeType, name, ST_AsText(geom) as wkt "
        + "from cp.`/sample-data/CA-cities.shp` where gid = 100")
    .ordered().baselineColumns("gid", "srid", "shapeType", "name", "wkt")
    .baselineValues(100, 4326, "Point", "Jenny Lind", "POINT (-120.8699371 38.0949216)")
    .build()
    .run();
  }

}