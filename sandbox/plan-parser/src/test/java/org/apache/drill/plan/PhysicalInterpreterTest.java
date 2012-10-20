/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.plan;

import org.apache.drill.plan.ast.Plan;
import org.junit.Test;

public class PhysicalInterpreterTest {
    @Test
    public void testTrivialPlan() throws PhysicalInterpreter.InterpreterException, ParsePlan.ParseException {
        run("physical-1.drillx");
    }

    @Test
    public void testExplodeFilter() throws PhysicalInterpreter.InterpreterException, ParsePlan.ParseException {
        run("physical-2.drillx");
    }

    private void run(String name) throws ParsePlan.ParseException, PhysicalInterpreter.SetupException, PhysicalInterpreter.QueryException {
        Plan p = ParsePlan.parseResource(name);
        PhysicalInterpreter pi = new PhysicalInterpreter(p);
        pi.run();
    }
}
