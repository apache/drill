/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.synth;

import com.google.common.collect.Lists;
import org.apache.mahout.math.random.Sampler;

import java.util.List;

/**
 * Samples from a set of things based on a long-tailed distribution.  This converts
 * the ChineseRestaurant distribution from a distribution over integers into a distribution
 * over more plausible looking things like words.
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
