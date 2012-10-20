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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.plan.ast.Arg;
import org.apache.drill.plan.ast.Op;
import org.apache.drill.plan.ast.Plan;
import org.apache.drill.plan.physical.operators.DataListener;
import org.apache.drill.plan.physical.operators.Operator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Takes a physical plan and interprets in locally.  The goal here is to provide a reference
 * semantics, not to provide a high speed evaluation of a query.
 */
public class PhysicalInterpreter implements DataListener {
    private final List<Operator> ops;

    public PhysicalInterpreter(Plan prog) throws SetupException {
        Map<Integer, Operator> bindings = Maps.newHashMap();
        ops = Lists.newArrayList();

        try {
            for (Op op : prog.getStatements()) {
                ops.add(Operator.create(op, bindings));
            }
        } catch (NoSuchMethodException e) {
            throw new SetupException(e);
        } catch (InvocationTargetException e) {
            throw new SetupException(e);
        } catch (IllegalAccessException e) {
            throw new SetupException(e);
        } catch (InstantiationException e) {
            throw new SetupException(e);
        }

        Iterator<Op> i = prog.getStatements().iterator();
        for (Operator op : ops) {
            op.link(i.next(), bindings);
        }

        Collection<Arg> outputs = prog.getOutputs();
        for (Arg output : outputs) {
            bindings.get(output.asSymbol().getInt()).addDataListener(this);
        }
    }

    public void run() throws QueryException {
        ExecutorService pool = Executors.newFixedThreadPool(ops.size());

        // pick out the ops that are tasks
        List<Callable<Object>> tasks = Lists.newArrayList(Iterables.transform(Iterables.filter(ops, new Predicate<Operator>() {
            @Override
            public boolean apply(@Nullable Operator operator) {
                return operator instanceof Callable;
            }
        }), new Function<Operator, Callable<Object>>() {
            @Override
            public Callable<Object> apply(@Nullable Operator operator) {
                // cast is safe due to previous filter
                return (Callable<Object>) operator;
            }
        }));

        List<Future<Object>> results;
        try {
            results = pool.invokeAll(tasks);
        } catch (InterruptedException e) {
            throw new QueryException(e);
        }
        pool.shutdown();

        for (Operator op : ops) {
            if (op instanceof Closeable) {
                op.close();
            }
        }

        try {
            Iterator<Callable<Object>> i = tasks.iterator();
            for (Future<Object> result : results) {
                System.out.printf("%s => %s\n", i.next(), result.get());
            }
        } catch (InterruptedException e) {
            throw new QueryException(e);
        } catch (ExecutionException e) {
            throw new QueryException(e);
        }
    }


    @Override
    public void notify(Object r) {
        System.out.printf("out = %s\n", r);
    }

    public static class InterpreterException extends Exception {
        private InterpreterException(Throwable throwable) {
            super(throwable);
        }
    }

    public static class SetupException extends InterpreterException {
        private SetupException(Throwable throwable) {
            super(throwable);
        }
    }

    public static class QueryException extends InterpreterException {
        private QueryException(Throwable throwable) {
            super(throwable);
        }
    }
}
