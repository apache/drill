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
