package org.apache.drill.plan.json;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

@JsonTypeName("explode")
public class Explode extends Consumer {
    private String source;
    private String target;
    private Consumer flow;

    public Explode(JsonObject spec) throws InvocationTargetException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        source = spec.get("source").getAsString();
        target = spec.get("target").getAsString();

        // this flow has to be an aggregator-flow.  This might happen because it is a sequence
        // whose last element is an aggregator.
        flow = Utils.create(spec.get("do").getAsJsonObject());
    }

    @Override
    public void push(Tuple t) throws InvocationTargetException, IOException {
        Object x = t.get(source);
        if (!(x instanceof Iterable)) {
            throw new IllegalArgumentException("Wanted Iterable, got %s" + x.getClass());
        }

        flow.start();
        Iterable<Tuple> data = (Iterable<Tuple>) x;
        for (Tuple tuple : data) {
            flow.push(tuple);
        }
        flow.end();

        t.put(target, flow.getAggregate());
    }
}
