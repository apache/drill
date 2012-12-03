package org.apache.drill.plan.json;

import com.google.common.collect.Lists;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
* Created with IntelliJ IDEA.
* User: tdunning
* Date: 11/19/12
* Time: 5:40 PM
* To change this template use File | Settings | File Templates.
*/
public class TupleCollector extends Consumer {
    private List<Tuple> tuples = Lists.newArrayList();

    @Override
    public void push(Tuple t) throws InvocationTargetException {
        tuples.add(t);
    }

    public List<Tuple> getTuples() {
        return tuples;
    }
}
