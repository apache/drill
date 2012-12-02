package org.apache.drill.plan.json;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: tdunning
 * Date: 11/19/12
 * Time: 5:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestUtils {
    static List<Tuple> runFlow(String json) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, IOException {
        JsonObject spec = new JsonParser().parse(json).getAsJsonObject();
        Consumer scanner = Utils.create(spec);
        TupleCollector tc = new TupleCollector();
        scanner.connect(tc);
        scanner.start();
        scanner.end();

        return tc.getTuples();
    }

    public static List<Tuple> runFlow(URL resource) throws IOException, InvocationTargetException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Joiner withNewlines = Joiner.on("\n");
        return runFlow(withNewlines.join(Resources.readLines(resource, Charsets.UTF_8)));
    }
}
