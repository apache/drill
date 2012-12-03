package org.apache.drill.plan.json;

import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public class ScanTest {
    @Test
    public void scan() throws InvocationTargetException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException {
        List<Tuple> tuples = TestUtils.runFlow("{type='scan', resource='test1.csv'}");
        Tuple x = tuples.get(0);
        assertEquals(1, (Integer) x.get("a"), 0);
        assertEquals("xyz", x.get("b"));
        assertEquals(3.1, (Double) x.get("c"), 0);

        x = tuples.get(1);
        assertEquals(2, (Integer) x.get("a"), 0);
        assertEquals("qrtz", x.get("b"));
        assertEquals(21, (Integer) x.get("c"), 0);
    }

}
