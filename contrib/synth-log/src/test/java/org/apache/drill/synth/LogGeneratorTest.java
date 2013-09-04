package org.apache.drill.synth;

import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: tdunning
 * Date: 2/3/13
 * Time: 2:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class LogGeneratorTest {
    @Test
    public void testSample() {
        LogGenerator gen = new LogGenerator();
        for (int i = 0; i < 1000; i++) {
            System.out.printf("%s\n", gen.sample());
        }
    }
}
