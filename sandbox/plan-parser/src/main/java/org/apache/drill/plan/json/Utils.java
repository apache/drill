package org.apache.drill.plan.json;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA. User: tdunning Date: 11/15/12 Time: 5:49 PM To change this template
 * use File | Settings | File Templates.
 */
public class Utils {
  private static Map<String, Class<? extends Consumer>> builtin = Maps.newHashMap();

  static {
    builtin.put("sequence", Sequence.class);
    builtin.put("filter", Filter.class);
    builtin.put("set", SetValue.class);
    builtin.put("scan", Scan.class);
    builtin.put("group", GroupBy.class);
  }

  public static Consumer create(JsonObject spec) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
    String type = spec.get("type").getAsString();
    Class<? extends Consumer> tClass;
    if (type != null) {
      tClass = builtin.get(type);
    } else {
      tClass = Class.forName(spec.get("class").getAsString()).asSubclass(Consumer.class);
    }

    return tClass.getConstructor(JsonObject.class).newInstance(spec);
  }

}
