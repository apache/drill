package org.apache.drill.plan.physical.operators;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.gson.*;

import javax.annotation.Nullable;
import java.util.Collection;

/**
* Created with IntelliJ IDEA.
* User: tdunning
* Date: 10/19/12
* Time: 5:01 PM
* To change this template use File | Settings | File Templates.
*/
public class JsonSchema extends Schema {
    Splitter onDot = Splitter.on(".");

    @Override
    public Object get(String name, Object data) {
        JsonElement r = (JsonElement) data;
        Iterable<String> bits = onDot.split(name);
        for (String bit : bits) {
            r = ((JsonObject) data).get(bit);
        }
        return cleanupJsonisms(r);
    }

    @Override
    public <T> Iterable<T> getIterable(String name, Object data) {
        Object r = get(name, data);
        if (r instanceof JsonArray) {
            return Iterables.transform((JsonArray) r, new Function<JsonElement, T>() {
                @Override
                public T apply(@Nullable JsonElement jsonElement) {
                    return (T) cleanupJsonisms(jsonElement);
                }
            });
        } else {
            throw new IllegalArgumentException("Looked for array but value was " + r.getClass());
        }
    }

    @Override
    public Schema getSubSchema(String name) {
        return new JsonSchema();
    }

    @Override
    public Schema overlay() {
        return this;
    }

    @Override
    public void set(String name, Object parent, Object value) {
        if (value instanceof Collection) {
            // input is likely to have been collected by Implode
            // TODO but what if something else built this?  Do we need a general serialization framework?
            JsonArray r = new JsonArray();
            for (Object v : ((Collection) value)) {
                r.add((JsonElement) v);
            }
            ((JsonObject) parent).add(name, r);
        } else if (value instanceof Number) {
            ((JsonObject) parent).add(name, new JsonPrimitive((Number) value));
        } else {
            throw new IllegalArgumentException(String.format("Can't convert a %s to JSON by magic", value.getClass()));
        }
    }

    private Object cleanupJsonisms(JsonElement data) {
        if (data instanceof JsonPrimitive) {
            JsonPrimitive v = (JsonPrimitive) data;
            if (v.isNumber()) {
                return v.getAsDouble();
            } else if (v.isString()) {
                return v.getAsString();
            } else {
                return v.getAsBoolean();
            }
        } else {
            return data;
        }
    }
}
