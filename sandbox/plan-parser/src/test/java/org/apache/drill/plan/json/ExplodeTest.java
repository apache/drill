package org.apache.drill.plan.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created with IntelliJ IDEA.
 * User: tdunning
 * Date: 11/20/12
 * Time: 4:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class ExplodeTest {
    @Test
    public void testSum() {

    }

    @Test
    public void testJackson() throws ClassNotFoundException, InvocationTargetException, IOException, NoSuchMethodException, IllegalAccessException, InstantiationException {
        Joiner withNewlines = Joiner.on("\n");
        JsonObject spec = new JsonParser().parse(withNewlines.join(Resources.readLines(Resources.getResource("explode-flow.json"), Charsets.UTF_8))).getAsJsonObject();
        Consumer scanner = Utils.create(spec);

        ObjectMapper mapper = new ObjectMapper();
        //ALLOW_UNQUOTED_FIELD_NAMES

        System.out.printf("%s\n", mapper.writeValueAsString(scanner));
    }
}
