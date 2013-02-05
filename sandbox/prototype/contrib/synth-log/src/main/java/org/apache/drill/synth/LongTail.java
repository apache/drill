package org.apache.drill.synth;

import com.google.common.collect.Lists;
import org.apache.mahout.math.random.ChineseRestaurant;
import org.apache.mahout.math.random.Sampler;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: tdunning
 * Date: 2/2/13
 * Time: 6:05 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class LongTail<T> implements Sampler<T> {
    private ChineseRestaurant base;
    private List<T> things = Lists.newArrayList();

    protected LongTail(double alpha, double discount) {
        base = new ChineseRestaurant(alpha, discount);
    }

    public T sample() {
        int n = base.sample();
        while (n >= things.size()) {
            things.add(createThing());
        }
        return things.get(n);
    }

    public ChineseRestaurant getBaseDistribution() {
        return base;
    }

    protected abstract T createThing();

    public List<T> getThings() {
        return things;
    }

    public void setThing(int i, T thing) {
        while (things.size() <= i) {
            things.add(null);
        }
        things.set(i, thing);
    }
}
