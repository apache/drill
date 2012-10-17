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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.plan.ast.Arg;
import org.apache.drill.plan.ast.Op;
import org.apache.drill.plan.ast.Plan;
import org.apache.drill.plan.physical.operators.DataListener;
import org.apache.drill.plan.physical.operators.Operator;
import org.apache.drill.plan.physical.operators.OperatorReference;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Takes a physical plan and interprets in locally.  The goal here is to provide a reference
 * semantics, not to provide a high speed evaluation of a query.
 */
public class PhysicalInterpreter implements DataListener {
    private final List<Operator> ops;

    public PhysicalInterpreter(Plan prog) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Map<Integer, OperatorReference> bindings = Maps.newHashMap();
        ops = Lists.newArrayList();

        for (Op op : prog.getStatements()) {
            ops.add(Operator.create(op, bindings));
        }

        Iterator<Op> i = prog.getStatements().iterator();
        for (Operator op : ops) {
            op.link(i.next(), bindings);
        }

        Collection<Arg> outputs = prog.getOutputs();
        for (Arg output : outputs) {
            bindings.get(output.asSymbol().getInt()).getOp().addDataListener(this);
        }
    }

    public void run() throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(ops.size());
        List<Future<Object>> tasks = pool.invokeAll(ops);
        pool.shutdown();

        for (Future<Object> task : tasks) {
            System.out.printf("%s\n", task.get());
        }
    }


    @Override
    public void notify(Object r) {
        System.out.printf("out = %s\n", r);
    }
}
