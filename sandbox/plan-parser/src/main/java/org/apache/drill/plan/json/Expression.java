package org.apache.drill.plan.json;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import org.codehaus.janino.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
* Created with IntelliJ IDEA.
* User: tdunning
* Date: 11/19/12
* Time: 4:43 PM
* To change this template use File | Settings | File Templates.
*/
class Expression<T> {
    private static Splitter onComma = Splitter.on(",");
    private JsonObject spec;
    private Class<? extends T> returnType;

    private ExpressionEvaluator expression;
    private String[] vars;

    public Expression(String expressionField, JsonObject formula, Class<? extends T> returnType) throws ClassNotFoundException, Scanner.ScanException, CompileException, Parser.ParseException {
        this.spec = formula;
        this.returnType = returnType;
        List<Class> types = Lists.newArrayList();
        vars = Iterables.toArray(onComma.split(formula.get("vars").getAsString()), String.class);
        String typeNames = formula.get("types").getAsString();
        if (typeNames != null) {
            for (String typeName : onComma.split(typeNames)) {
                if (typeName.equals("double")) {
                    types.add(double.class);
                } else if (typeName.equals("int")) {
                    types.add(int.class);
                } else if (typeName.equals("String")) {
                    types.add(int.class);
                } else {
                    types.add(Class.forName(typeName));
                }
            }
        } else {
            for (String var : vars) {
                types.add(double.class);
            }
        }

        expression = new ExpressionEvaluator(
                formula.get(expressionField).getAsString(),
                returnType,
                vars,
                Iterables.toArray(types, Class.class)
        );
    }

    public T eval(Tuple t) throws InvocationTargetException {
        List<Object> values = Lists.newArrayList();
        for (String var : vars) {
            values.add(t.get(var));
        }
        return (T) expression.evaluate(values.toArray());
    }

}
