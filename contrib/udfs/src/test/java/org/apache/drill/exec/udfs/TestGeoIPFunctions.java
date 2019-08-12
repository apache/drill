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

package org.apache.drill.exec.udfs;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({UnlikelyTest.class, SqlFunctionTest.class})
public class TestGeoIPFunctions extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testGetCountryName() throws Exception {
    String query = "select getCountryName('81.169.181.179') as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues("Germany").go();

    query = "select getCountryName('81.169.181') as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues("Belgium").go();

    query = "select getCountryName('') as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues("Unknown").go();

    query = "select getCountryName(cast(null as varchar)) as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues((String) null).go();
  }

  @Test
  public void testGetCountryISOCode() throws Exception {
    String query = "select getCountryISOCode('81.169.181.179') as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues("DE").go();

    query = "select getCountryISOCode('81.169.181') as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues("BE").go();

    query = "select getCountryISOCode('') as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues("UNK").go();

    query = "select getCountryISOCode(cast(null as varchar)) as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues((String) null).go();
  }

  @Test
  public void testGetCountryConfidence() throws Exception {
    String query = "select getCountryConfidence('81.169.181.179') as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues(0).go();

    query = "select getCountryConfidence('81.169.181') as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues(0).go();

    query = "select getCountryConfidence('') as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues(0).go();

    query = "select getCountryConfidence(cast(null as varchar)) as country from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("country").baselineValues((String) null).go();
  }

  @Test
  public void testGetCityName() throws Exception {
    String query = "select getCityName('81.169.181.179') as city from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("city").baselineValues("Berlin").go();

    query = "select getCityName('81.169.181') as city from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("city").baselineValues("Charleroi").go();

    query = "select getCityName('') as city from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("city").baselineValues("Unknown").go();

    query = "select getCityName(cast(null as varchar)) as city from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("city").baselineValues((String) null).go();
  }

  @Test
  public void testGetCityConfidence() throws Exception {
    String query = "select getCityConfidence('81.169.181.179') as city from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("city").baselineValues(0).go();

    query = "select getCityConfidence('81.169.181') as city from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("city").baselineValues(0).go();

    query = "select getCityConfidence('') as city from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("city").baselineValues(0).go();

    query = "select getCityConfidence(cast(null as varchar)) as city from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("city").baselineValues((Integer) null).go();
  }

  @Test
  public void testGetLatLong() throws Exception {
    String query = "select getLatitude('81.169.181.179') as latitude,  getLongitude('81.169.181.179') as longitude from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("latitude", "longitude").baselineValues(52.5167, 13.4).go();

    query = "select getLatitude('81.169.181') as latitude,  getLongitude('81.169.181') as longitude from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("latitude", "longitude").baselineValues(50.4167, 4.4333).go();

    query = "select getLatitude('') as latitude,  getLongitude('') as longitude from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("latitude", "longitude").baselineValues(0.0, 0.0).go();

    query = "select getLatitude(cast(null as varchar)) as latitude,  getLongitude(cast(null as varchar)) as longitude from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("latitude", "longitude").baselineValues((Integer) null, (Integer) null).go();
  }


  @Test
  public void testGetTimeZone() throws Exception {
    String query = "select getTimezone('81.169.181.179') as tz from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("tz").baselineValues("Europe/Berlin").go();

    query = "select getTimezone('81.169.181') as tz from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("tz").baselineValues("Europe/Brussels").go();

    query = "select getTimezone('') as tz from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("tz").baselineValues("Unknown").go();

    query = "select getTimezone(cast(null as varchar)) as tz from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("tz").baselineValues((Integer) null).go();
  }

  @Test
  public void testGetAccuracyRadius() throws Exception {
    String query = "select getAccuracyRadius('81.169.181.179') as radius from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("radius").baselineValues(200).go();

    query = "select getAccuracyRadius('81.169.181') as radius from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("radius").baselineValues(100).go();

    query = "select getAccuracyRadius('') as radius from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("radius").baselineValues(0).go();

    query = "select getAccuracyRadius(cast(null as varchar)) as radius from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("radius").baselineValues((Integer) null).go();
  }

  @Test
  public void testGetAverageIncome() throws Exception {
    String query = "select getAverageIncome('81.169.181.179') as radius from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("radius").baselineValues(0).go();

    query = "select getAverageIncome('81.169.181') as radius from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("radius").baselineValues(0).go();

    query = "select getAverageIncome('') as radius from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("radius").baselineValues(0).go();

    query = "select getAverageIncome(cast(null as varchar)) as radius from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("radius").baselineValues((Integer) null).go();
  }

  @Test
  public void testGetMetroCode() throws Exception {
    String query = "select getMetroCode('81.169.181.179') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(0).go();

    query = "select getMetroCode('81.169.181') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(0).go();

    query = "select getMetroCode('') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(0).go();

    query = "select getMetroCode(cast(null as varchar)) as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues((Integer) null).go();
  }

  @Test
  public void testGetPopulationDensity() throws Exception {
    String query = "select getPopulationDensity('81.169.181.179') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(0).go();

    query = "select getPopulationDensity('81.169.181') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(0).go();

    query = "select getPopulationDensity('') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(0).go();

    query = "select getPopulationDensity(cast(null as varchar)) as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues((Integer) null).go();
  }

  @Test
  public void testIsEU() throws Exception {
    String query = "select isEU('81.169.181.179') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(true).go();

    query = "select isEU('81.169.181') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(true).go();

    query = "select isEU('') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isEU(cast(null as varchar)) as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues((Integer) null).go();
  }

  @Test
  public void testGetPostalCode() throws Exception {
    String query = "select getPostalCode('81.169.181.179') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues("12529").go();

    query = "select getPostalCode('81.169.181') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues("6001").go();

    query = "select getPostalCode('') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues("Unknown").go();

    query = "select getPostalCode(cast(null as varchar)) as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues((String) null).go();
  }

  // TODO Test Geo Point

  @Test
  public void testASN() throws Exception {
    String query = "select getASN('81.169.181.179') as result from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("result").baselineValues(6724).go();

    query = "select getASN('81.169.181') as result from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("result").baselineValues(5432).go();

    query = "select getASN('') as result from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("result").baselineValues(0).go();

    query = "select getASN(cast(null as varchar)) as result from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("result").baselineValues((Integer) null).go();
  }

  @Test
  public void testASNOrg() throws Exception {
    String query = "select getASNOrganization('81.169.181.179') as result from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("result").baselineValues("Strato AG").go();

    query = "select getASNOrganization('81.169.181') as result from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("result").baselineValues("Proximus NV").go();

    query = "select getASNOrganization('') as result from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("result").baselineValues("Unknown").go();

    query = "select getASNOrganization(cast(null as varchar)) as result from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("result").baselineValues((String) null).go();
  }

  @Test
  public void testIsAnonymous() throws Exception {
    String query = "select isAnonymous('81.169.181.179') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isAnonymous('81.169.181') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isAnonymous('') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isAnonymous(cast(null as varchar)) as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues((Integer) null).go();
  }

  @Test
  public void testIsAnonymousVPN() throws Exception {
    String query = "select isAnonymousVPN('81.169.181.179') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isAnonymousVPN('81.169.181') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isAnonymousVPN('') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isAnonymousVPN(cast(null as varchar)) as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues((Integer) null).go();
  }

  @Test
  public void testIsHostingProvider() throws Exception {
    String query = "select isHostingProvider('81.169.181.179') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isHostingProvider('81.169.181') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isHostingProvider('') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isHostingProvider(cast(null as varchar)) as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues((Integer) null).go();
  }

  @Test
  public void testIsPublicProxy() throws Exception {
    String query = "select isPublicProxy('81.169.181.179') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isPublicProxy('81.169.181') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isPublicProxy('') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isPublicProxy(cast(null as varchar)) as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues((Integer) null).go();
  }

  @Test
  public void testIsTOR() throws Exception {
    String query = "select isTORExitNode('81.169.181.179') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isTORExitNode('81.169.181') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isTORExitNode('') as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues(false).go();

    query = "select isTORExitNode(cast(null as varchar)) as mc from (values(1))";
    testBuilder().sqlQuery(query).ordered().baselineColumns("mc").baselineValues((Integer) null).go();
  }
}
