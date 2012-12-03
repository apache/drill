package org.apache.drill.plan.json;

import java.lang.reflect.InvocationTargetException;

/**
 * Created with IntelliJ IDEA.
 * User: tdunning
 * Date: 11/16/12
 * Time: 3:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class SingleObjectCatcher extends Consumer {
    Tuple result = null;
    @Override
    public void push(Tuple t) throws InvocationTargetException {
        if (result == null) {
            result = t;
        } else {
            throw new RuntimeException("Flow produced more than one output");
        }
    }

    public Tuple getResult() {
        return result;
    }
}
