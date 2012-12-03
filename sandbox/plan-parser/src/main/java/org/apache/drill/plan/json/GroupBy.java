package org.apache.drill.plan.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.*;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

@JsonTypeName("group")
public class GroupBy extends Consumer {
    private String key;
    private String target;

    private Multimap<String, Tuple> groups = HashMultimap.create();
    private Consumer out;

    public GroupBy(JsonObject spec) {
        key = spec.get("by").getAsString();
        target = spec.get("as").getAsString();
    }

    @Override
    public void connect(Consumer out) {
        this.out = out;
    }

    @Override
    public void push(Tuple t) throws InvocationTargetException, IOException {
        String keyValue = (String) t.get(key);
        groups.put(keyValue, t);
    }

    @Override
    public void end() throws InvocationTargetException, IOException {
        for (String keyValue : groups.keySet()) {
            Tuple r = new MapTuple();
            r.put(key, keyValue);
            r.put(target, groups.get(keyValue));
            out.push(r);
        }
    }

    public String getKey() {
        return key;
    }

    @JsonProperty("as")
    public String getTarget() {
        return target;
    }
}
