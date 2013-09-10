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

import com.google.common.base.Function;
import com.google.common.collect.*;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.mahout.math.stats.LogLikelihood;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TermGeneratorTest {

    private static final WordGenerator WORDS = new WordGenerator("word-frequency-seed", "other-words");

    @Test
    public void generateTerms() {
        TermGenerator x = new TermGenerator(WORDS, 1, 0.8);
        final Multiset<String> counts = HashMultiset.create();
        for (int i = 0; i < 10000; i++) {
            counts.add(x.sample());
        }

        assertEquals(10000, counts.size());
        assertTrue("Should have some common words", counts.elementSet().size() < 10000);
        List<Integer> k = Lists.newArrayList(Iterables.transform(counts.elementSet(), new Function<String, Integer>() {
            public Integer apply(String s) {
                return counts.count(s);
            }
        }));
//        System.out.printf("%s\n", Ordering.natural().reverse().sortedCopy(k).subList(0, 30));
//        System.out.printf("%s\n", Iterables.transform(Iterables.filter(counts.elementSet(), new Predicate<String>() {
//            public boolean apply(String s) {
//                return counts.count(s) > 100;
//            }
//        }), new Function<String, String>() {
//            public String apply(String s) {
//                return s + ":" + counts.count(s);
//            }
//        }));
        assertEquals(1, Ordering.natural().leastOf(k, 1).get(0).intValue());
        assertTrue(Ordering.natural().greatestOf(k, 1).get(0) > 300);
        assertTrue(counts.count("the") > 300);
    }

    @Test
    public void distinctVocabularies() {
        TermGenerator x1 = new TermGenerator(WORDS, 1, 0.8);
        final Multiset<String> k1 = HashMultiset.create();
        for (int i = 0; i < 50000; i++) {
            k1.add(x1.sample());
        }

        TermGenerator x2 = new TermGenerator(WORDS, 1, 0.8);
        final Multiset<String> k2 = HashMultiset.create();
        for (int i = 0; i < 50000; i++) {
            k2.add(x2.sample());
        }

        final NormalDistribution normal = new NormalDistribution();
        List<Double> scores = Ordering.natural().sortedCopy(Iterables.transform(k1.elementSet(),
                new Function<String, Double>() {
                    public Double apply(String s) {
                        return normal.cumulativeProbability(LogLikelihood.rootLogLikelihoodRatio(k1.count(s), 50000 - k1.count(s), k2.count(s), 50000 - k2.count(s)));
                    }
                }));
        int n = scores.size();
//        System.out.printf("%.5f, %.5f, %.5f, %.5f, %.5f, %.5f, %.5f", scores.get(0), scores.get((int) (0.05*n)), scores.get(n / 4), scores.get(n / 2), scores.get(3 * n / 4), scores.get((int) (0.95 * n)), scores.get(n - 1));
        int i = 0;
        for (Double score : scores) {
            if (i % 10 == 0) {
                System.out.printf("%.6f\t%.6f\n", (double) i / n, score);
            }

            i++;
        }
    }
}
