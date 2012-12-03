package org.apache.drill.plan.json;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.gson.JsonObject;
import org.codehaus.janino.CompileException;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

@JsonTypeName("filter")
public class Filter extends Consumer {
    private Consumer out;
    private Expression<Boolean> expression;

    public Filter(JsonObject spec) throws Scanner.ScanException, CompileException, Parser.ParseException, ClassNotFoundException {
        expression = new Expression<Boolean>("if", spec, Boolean.class);
    }

    @Override
    public void connect(Consumer out) {
        this.out = out;
    }

    @Override
    public void push(Tuple t) throws InvocationTargetException, IOException {
        if (expression.eval(t)) {
            out.push(t);
        }
    }

}
