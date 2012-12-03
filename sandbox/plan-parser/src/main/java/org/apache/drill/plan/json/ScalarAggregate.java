package org.apache.drill.plan.json;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: tdunning
 * Date: 11/20/12
 * Time: 4:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class ScalarAggregate extends Consumer {
    private static final Map<String, Class<? extends AggregationFunction>> aggregators = ImmutableMap.of("sum", Sum.class, "count", Count.class);

    private double result = 0;
    private AggregationFunction f;
    private final String sourceVariable;


    public ScalarAggregate(JsonObject spec) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        f = aggregators.get(spec.get("update").getAsString()).newInstance();
        sourceVariable = spec.get("source").getAsString();
    }

    @Override
    public void push(Tuple t) throws InvocationTargetException, IOException {
        Object v = t.get(sourceVariable);
        if (v instanceof Double) {
            f.update((Double) v);
        } else if (v instanceof Integer) {
            f.update((Integer) v);
        } else {
            throw new IllegalArgumentException("Wanted a number in " + sourceVariable + " but got a " + v.getClass());
        }
    }

    @Override
    public Object getAggregate() {
        return f.value();
    }

    private static abstract class AggregationFunction {
        protected double value;

        abstract void update(double x);

        protected double value() {
            return value;
        }
    }

    private static class Sum extends AggregationFunction {
        @Override
        void update(double x) {
            value += x;
        }
    }

    private static class Count extends AggregationFunction {
        @Override
        void update(double x) {
            value += 1;
        }
    }
}
