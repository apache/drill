package org.apache.drill.plan.json;

import com.google.common.io.Resources;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: tdunning
 * Date: 11/19/12
 * Time: 5:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class FilterTest {
    @Test
    public void integerTest() throws IOException, InvocationTargetException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        List<Tuple> tuples = TestUtils.runFlow(Resources.getResource("filter-flow.json"));
        assertEquals(1, tuples.size());
        assertEquals(2, tuples.get(0).get("a"));
    }
}
