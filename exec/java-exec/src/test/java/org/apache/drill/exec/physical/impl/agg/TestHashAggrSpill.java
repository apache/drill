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

package org.apache.drill.exec.physical.impl.agg;

import ch.qos.logback.classic.Level;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.aggregate.HashAggTemplate;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.FixtureBuilder;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.ProfileParser;
import org.apache.drill.test.QueryBuilder;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *  Test spilling for the Hash Aggr operator (using the mock reader)
 */
public class TestHashAggrSpill extends BaseTestQuery {

    private void runAndDump(ClientFixture client, String sql, long expectedRows, long spillCycle, long spilledPartitions) throws Exception {
        String plan = client.queryBuilder().sql(sql).explainJson();

        QueryBuilder.QuerySummary summary = client.queryBuilder().sql(sql).run();
        if ( expectedRows > 0 ) {
            assertEquals(expectedRows, summary.recordCount());
        }
        // System.out.println(String.format("======== \n Results: %,d records, %d batches, %,d ms\n ========", summary.recordCount(), summary.batchCount(), summary.runTimeMs() ) );

        //System.out.println("Query ID: " + summary.queryIdString());
        ProfileParser profile = client.parseProfile(summary.queryIdString());
        //profile.print();
        List<ProfileParser.OperatorProfile> ops = profile.getOpsOfType(UserBitShared.CoreOperatorType.HASH_AGGREGATE_VALUE);

        assertTrue( ! ops.isEmpty() );
        // check for the first op only
        ProfileParser.OperatorProfile hag0 = ops.get(0);
        long opCycle = hag0.getMetric(HashAggTemplate.Metric.SPILL_CYCLE.ordinal());
        assertEquals(spillCycle, opCycle);
        long op_spilled_partitions = hag0.getMetric(HashAggTemplate.Metric.SPILLED_PARTITIONS.ordinal());
        assertEquals(spilledPartitions, op_spilled_partitions);
        /* assertEquals(3, ops.size());
        for ( int i = 0; i < ops.size(); i++ ) {
            ProfileParser.OperatorProfile hag = ops.get(i);
            long cycle = hag.getMetric(HashAggTemplate.Metric.SPILL_CYCLE.ordinal());
            long num_partitions = hag.getMetric(HashAggTemplate.Metric.NUM_PARTITIONS.ordinal());
            long spilled_partitions = hag.getMetric(HashAggTemplate.Metric.SPILLED_PARTITIONS.ordinal());
            long mb_spilled = hag.getMetric(HashAggTemplate.Metric.SPILL_MB.ordinal());
            System.out.println(String.format("(%d) Spill cycle: %d, num partitions: %d, spilled partitions: %d, MB spilled: %d", i,cycle, num_partitions, spilled_partitions,
                mb_spilled));
        } */
    }

    /**
     * Test "normal" spilling: Only 2 partitions (out of 4) would require spilling
     * ("normal spill" means spill-cycle = 1 )
     *
     * @throws Exception
     */
    @Test
    public void testHashAggrSpill() throws Exception {
        LogFixture.LogFixtureBuilder logBuilder = LogFixture.builder()
            .toConsole()
            .logger("org.apache.drill.exec.physical.impl.aggregate", Level.DEBUG)
            ;

        FixtureBuilder builder = ClusterFixture.builder()
            .configProperty(ExecConstants.HASHAGG_MAX_MEMORY,74_000_000)
            .configProperty(ExecConstants.HASHAGG_NUM_PARTITIONS,16)
            .configProperty(ExecConstants.HASHAGG_MIN_BATCHES_PER_PARTITION,3)
            .sessionOption(PlannerSettings.FORCE_2PHASE_AGGR_KEY,true)
            // .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
            .maxParallelization(2)
            .saveProfiles()
            //.keepLocalFiles()
            ;
        try (LogFixture logs = logBuilder.build();
             ClusterFixture cluster = builder.build();
             ClientFixture client = cluster.clientFixture()) {
            String sql = "SELECT empid_s17, dept_i, branch_i, AVG(salary_i) FROM `mock`.`employee_1200K` GROUP BY empid_s17, dept_i, branch_i";
            runAndDump(client, sql, 1_200_000, 1, 1);
        }
    }

    /**
     *  Test "secondary" spilling -- Some of the spilled partitions cause more spilling as they are read back
     *  (Hence spill-cycle = 2 )
     *
     * @throws Exception
     */
    @Test
    public void testHashAggrSecondaryTertiarySpill() throws Exception {
        LogFixture.LogFixtureBuilder logBuilder = LogFixture.builder()
            .toConsole()
            .logger("org.apache.drill.exec.physical.impl.aggregate", Level.DEBUG)
            .logger("org.apache.drill.exec.cache", Level.INFO)
            ;

        FixtureBuilder builder = ClusterFixture.builder()
            .configProperty(ExecConstants.HASHAGG_MAX_MEMORY,58_000_000)
            .configProperty(ExecConstants.HASHAGG_NUM_PARTITIONS,16)
            .configProperty(ExecConstants.HASHAGG_MIN_BATCHES_PER_PARTITION,3)
            .sessionOption(PlannerSettings.FORCE_2PHASE_AGGR_KEY,true)
            .sessionOption(PlannerSettings.STREAMAGG.getOptionName(),false)
            // .sessionOption(PlannerSettings.EXCHANGE.getOptionName(), true)
            .maxParallelization(1)
            .saveProfiles()
            //.keepLocalFiles()
            ;
        try (LogFixture logs = logBuilder.build();
             ClusterFixture cluster = builder.build();
             ClientFixture client = cluster.clientFixture()) {
            String sql = "SELECT empid_s44, dept_i, branch_i, AVG(salary_i) FROM `mock`.`employee_1100K` GROUP BY empid_s44, dept_i, branch_i";
            runAndDump(client, sql, 1_100_000, 3, 2);
        }
    }
}