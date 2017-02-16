/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.store.indexr;

import org.apache.drill.BaseTestQuery;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("requires indexr node setting up")
public class TestIndexRPlugin extends BaseTestQuery {

  @Test
  public void testIndexr() throws Exception {
    System.setProperty("saffron.default.charset", "UTF-16LE");

    //String sql = "select A.user_id, sum(A.clicks), sum(B.impressions) as aa from indexr.campaign as A join indexr.campaign as B on A.channel_id = B.channel_id " +
    //    "where " +
    //    "A.campaign_id = 100000000 and B.user_id = 20 " +
    //    "group by A.user_id order by aa desc limit 100";
    //
    //sql = "select spot_id, sum(campaign_id), sum(impressions), sum(cost) " +
    //    "from indexr.campaign where user_id in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19) and campaign_id > 10 " +
    //    "group by spot_id order by sum(cost) desc limit 10";

    String sql = "select * from indexr.test where `date` = '2017-02-14' limit 10";

    //sql = "select * from indexr.test limit 100";

    //sql = "SELECT `package_id` FROM indexr.campaign GROUP BY `package_id` ORDER BY (CAST(sum(`cost_over2`) AS DOUBLE) / NULLIF(100, 0)) DESC NULLS LAST";

    //test("explain plan for " + sql);
    test(sql);
  }

}
