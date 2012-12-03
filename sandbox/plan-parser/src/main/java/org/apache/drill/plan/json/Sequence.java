package org.apache.drill.plan.json;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Created with IntelliJ IDEA. User: tdunning Date: 11/15/12 Time: 5:36 PM To change this template
 * use File | Settings | File Templates.
 */
@JsonTypeName("sequence")
public class Sequence<R> extends Consumer {
    private List<Consumer> step = Lists.newArrayList();
    private Consumer lastStep;
    private Consumer out;

    public Sequence(JsonObject spec) throws InvocationTargetException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        for (JsonElement element : spec.getAsJsonArray("do")) {
            step.add(Utils.create(element.getAsJsonObject()));
        }
        lastStep = step.get(step.size() - 1);
    }

    @Override
    public void connect(Consumer out) {
        this.out = out;
        for (int i = 1; i < step.size(); i++) {
            step.get(i - 1).connect(step.get(i));
        }
        step.get(step.size() - 1).connect(out);
    }

    @Override
    public void start() throws IOException {
        for (Consumer consumer : step) {
            consumer.start();
        }
        out.start();
    }

    @Override
    public void push(Tuple t) throws InvocationTargetException, IOException {
        step.get(0).push(t);
    }

    @Override
    public void end() throws InvocationTargetException, IOException {
        for (Consumer s : step) {
            s.end();
        }
    }

    @Override
    public Object getAggregate() {
        return lastStep.getAggregate();
    }

    public List<Consumer> getDo() {
        return step;
    }
}
