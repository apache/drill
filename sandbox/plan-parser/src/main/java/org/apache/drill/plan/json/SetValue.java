package org.apache.drill.plan.json;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import org.codehaus.janino.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Created with IntelliJ IDEA. User: tdunning Date: 11/15/12 Time: 6:10 PM To change this template
 * use File | Settings | File Templates.
 */

@JsonTypeName("set")
public class SetValue extends Consumer {
    private Consumer out;
    private String target;
    private Expression<Object> expression;

    public SetValue(JsonObject spec) throws Scanner.ScanException, CompileException, Parser.ParseException, ClassNotFoundException {
        Splitter onComma = Splitter.on(",");

        String targetTypeName = spec.get("targetType").getAsString();
        Class<?> targetType;
        if (targetTypeName != null) {
            targetType = Class.forName(targetTypeName).asSubclass(Object.class);
        } else {
            targetType = double.class;
        }

        expression = new Expression<Object>("value", spec, targetType);

        target = spec.get("as").getAsString();
    }

    @Override
    public void connect(Consumer out) {
        this.out = out;
    }

    @Override
    public void push(Tuple t) throws InvocationTargetException, IOException {
        t.put(target, expression.eval(t));
        out.push(t);
    }
}
