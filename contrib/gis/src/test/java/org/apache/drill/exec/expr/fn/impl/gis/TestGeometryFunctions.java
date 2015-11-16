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

  @Test
  public void testSTXQuery() throws Exception {

    testBuilder()
      .sqlQuery("select ST_X(ST_Point(-121.895, 37.339)) "
          + "from cp.`/sample-data/CA-cities.csv` limit 1")
      .ordered().baselineColumns("EXPR$0")
      .baselineValues(-121.895)
      .build()
      .run();
  }

  @Test
  public void testSTYQuery() throws Exception {

    testBuilder()
      .sqlQuery("select ST_Y(ST_Point(-121.895, 37.339)) "
          + "from cp.`/sample-data/CA-cities.csv` limit 1")
      .ordered().baselineColumns("EXPR$0")
      .baselineValues(37.339)
      .build()
      .run();
  }
  

  @Test
  public void testSTX_STYGivesNaNForNonPointGeometry() throws Exception {

    testBuilder()
      .sqlQuery("select ST_X(ST_GeomFromText('MULTIPOINT((16 64))')) "
          + "from (VALUES(1))")
      .ordered().baselineColumns("EXPR$0")
      .baselineValues(Double.NaN)
      .build()
      .run();

    testBuilder()
    .sqlQuery("select ST_Y(ST_GeomFromText('MULTIPOINT((16 64))')) "
        + "from (VALUES(1))")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(Double.NaN)
    .build()
    .run();
  }


  @Test
  public void testIntersectQuery() throws Exception {

    testBuilder()
      .sqlQuery("SELECT ST_Intersects(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING(2 0,0 2)')) "
          + "from (VALUES(1))")
      .ordered().baselineColumns("EXPR$0")
      .baselineValues(false)
      .build()
      .run();
    
    testBuilder()
    .sqlQuery("SELECT ST_Intersects(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING(0 0,0 2)')) "
        + "from (VALUES(1))")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(true)
    .build()
    .run();
  }

  @Test
  public void testRelateQuery() throws Exception {

    testBuilder()
      .sqlQuery("SELECT ST_Relate(ST_GeomFromText('POINT(1 2)'), ST_Buffer(ST_GeomFromText('POINT(1 2)'),2), '0FFFFF212') "
          + "from (VALUES(1))")
      .ordered().baselineColumns("EXPR$0")
      .baselineValues(true)
      .build()
      .run();

    testBuilder()
    .sqlQuery("SELECT ST_Relate(ST_GeomFromText('POINT(1 2)'), ST_Buffer(ST_GeomFromText('POINT(1 2)'),2), '*FF*FF212') "
        + "from (VALUES(1))")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(true)
    .build()
    .run();

    testBuilder()
    .sqlQuery("SELECT ST_Relate(ST_GeomFromText('POINT(0 0)'), ST_Buffer(ST_GeomFromText('POINT(1 2)'),2), '*FF*FF212') "
        + "from (VALUES(1))")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(false)
    .build()
    .run();

  }

  @Test
  public void testTouchesQuery() throws Exception {

    testBuilder()
      .sqlQuery("SELECT ST_Touches(ST_GeomFromText('LINESTRING(0 0, 1 1, 0 2)'), ST_GeomFromText('POINT(1 1)')) "
          + "from (VALUES(1))")
      .ordered().baselineColumns("EXPR$0")
      .baselineValues(false)
      .build()
      .run();

    testBuilder()
    .sqlQuery("SELECT ST_Touches(ST_GeomFromText('LINESTRING(0 0, 1 1, 0 2)'), ST_GeomFromText('POINT(0 2)')) "
        + "from (VALUES(1))")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(true)
    .build()
    .run();

  }

  @Test
  public void testEqualsQuery() throws Exception {

    testBuilder()
      .sqlQuery("SELECT ST_Equals(ST_GeomFromText('LINESTRING(0 0, 10 10)'), "
                + "ST_GeomFromText('LINESTRING(0 0, 5 5, 10 10)')) from (VALUES(1))")
      .ordered().baselineColumns("EXPR$0")
      .baselineValues(true)
      .build()
      .run();

  }

  @Test
  public void testContainsQuery() throws Exception {

    testBuilder()
      .sqlQuery("SELECT ST_Contains(smallc, bigc) As smallcontainsbig, "
                     + "ST_Contains(bigc,smallc) As bigcontainssmall, "
                     + "ST_Contains(bigc, ST_Union(smallc, bigc)) as bigcontainsunion, "
                     + "ST_Equals(bigc, ST_Union(smallc, bigc)) as bigisunion "
                + "FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc, "
                       + "ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc from (VALUES(1)) ) As foo")
      .ordered().baselineColumns("smallcontainsbig", "bigcontainssmall", "bigcontainsunion", "bigisunion")
      .baselineValues(false, true, true, true)
      .build()
      .run();

  }

  @Test
  public void testOverlapsCrossesIntersectsContainsQuery() throws Exception {

    testBuilder()
      .sqlQuery("SELECT ST_Overlaps(a,b) As a_overlap_b, "
                  + "ST_Crosses(a,b) As a_crosses_b, "
                  + "ST_Intersects(a, b) As a_intersects_b, "
                  + "ST_Contains(b,a) As b_contains_a "
                + "FROM (SELECT ST_GeomFromText('POINT(1 0.5)') As a, ST_GeomFromText('LINESTRING(1 0, 1 1, 3 5)')  As b "
                  + "from (VALUES(1)) ) As foo")
      .ordered().baselineColumns("a_overlap_b", "a_crosses_b", "a_intersects_b", "b_contains_a")
      .baselineValues(false, false, true, true)
      .build()
      .run();

  }

  @Test
  public void testDisjointQuery() throws Exception {

    testBuilder()
      .sqlQuery("SELECT ST_Disjoint(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING( 2 0, 0 2 )')) "
                + "from (VALUES(1))")
      .ordered().baselineColumns("EXPR$0")
      .baselineValues(true)
      .build()
      .run();

    testBuilder()
    .sqlQuery("SELECT ST_Disjoint(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING( 0 0, 0 2 )')) "
              + "from (VALUES(1))")
    .ordered().baselineColumns("EXPR$0")
    .baselineValues(false)
    .build()
    .run();

  }
}