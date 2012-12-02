package org.apache.drill.plan.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created with IntelliJ IDEA. User: tdunning Date: 11/15/12 Time: 5:43 PM To change this template
 * use File | Settings | File Templates.
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.NAME, include= JsonTypeInfo.As.PROPERTY, property="type")
 public abstract class Consumer {
    public abstract void push(Tuple t) throws InvocationTargetException, IOException;

    public void connect(Consumer out) {
        throw new UnsupportedOperationException("Must over-ride initialize in non-source class");
    }

    public void start() throws IOException {
        // by default do nothing
    }

    public void end() throws InvocationTargetException, IOException {
        // by default do nothing
    }

    @JsonIgnore
    public Object getAggregate() {
        throw new UnsupportedOperationException("Not an aggregator");
    }
}
