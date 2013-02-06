package org.apache.drill.synth;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WordGeneratorTest {
    @Test
    public void checkRealWords() {
        WordGenerator words = new WordGenerator("word-frequency-seed", "other-words");
        for (int i = 0; i < 20000; i++) {
            assertFalse(words.getString(i).matches("-[0-9]+"));
        }

        for (int i = 0; i < 1000; i++) {
            String w = words.getString(i + 200000);
            assertTrue(w.matches(".*-[0-9]+"));
        }
    }

}
