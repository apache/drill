package org.apache.drill.plan.json;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: tdunning
 * Date: 11/19/12
 * Time: 5:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class MapTuple extends HashMap<String, Object> implements Tuple {
    public MapTuple(Iterable<String> keys, Iterable<String> values) {
        Iterator<String> i = keys.iterator();
        Iterator<String> j = values.iterator();
        while (i.hasNext()) {
            String v = j.next();
            Object x;
            try {
                x = Integer.parseInt(v);
            } catch (NumberFormatException e) {
                try {
                    x = Double.parseDouble(v);
                } catch (NumberFormatException ex) {
                    x = v;
                }
            }
            put(i.next(), x);
        }
    }

    public MapTuple() {
        super();
    }
}
