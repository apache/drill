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

import org.apache.mahout.math.random.Sampler;

/**
 * Generate words at random from a specialized vocabulary.  Every term generator's
 * frequency distribution has a common basis, but each will diverge after initialization.
 */
public class TermGenerator implements Sampler<String> {
    // the word generator handles the problem of making up new words
    // it also provides the seed frequencies
    private WordGenerator words;

    private LongTail<String> distribution;

    public TermGenerator(WordGenerator words, final int alpha, final double discount) {
        this.words = words;
        distribution = new LongTail<String>(alpha, discount) {
            private int count = TermGenerator.this.words.size();

            @Override
            protected String createThing() {
                return TermGenerator.this.words.getString(count++);
            }
        };

        int i = 0;
        for (String word : this.words.getBaseWeights().keySet()) {
            distribution.getBaseDistribution().setCount(i, this.words.getBaseWeights().get(word));
            distribution.setThing(i, word);
            i++;
        }

    }

    public String sample() {
        return distribution.sample();
    }
}
