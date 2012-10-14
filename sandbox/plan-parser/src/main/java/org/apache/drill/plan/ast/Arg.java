package org.apache.drill.plan.ast;

import com.google.common.base.CharMatcher;

/**
 * Created with IntelliJ IDEA. User: tdunning Date: 10/12/12 Time: 11:40 PM To change this template
 * use File | Settings | File Templates.
 */
public class Arg {
    private static final CharMatcher quotes = CharMatcher.is('"');
    private static final CharMatcher percent = CharMatcher.is('%');

    public static Arg createString(String s) {
        return new QuotedString(s);
    }

    public static Arg createNumber(String n) {
        return new Number(Double.parseDouble(n));
    }

    public static Arg createSymbol(String s) {
        return new Symbol(Integer.parseInt(percent.trimLeadingFrom(s)));
    }

    public static Arg createBoolean(String b) {
        return new BooleanConstant(Boolean.parseBoolean(b));
    }

    public static class QuotedString extends Arg {
        private String s;

        public QuotedString(String s) {
            this.s = quotes.trimFrom(quotes.trimFrom(s));
        }
    }

    public static class BooleanConstant extends Arg {
        private boolean v;

        public BooleanConstant(boolean b) {
            v = b;
        }
    }

    public static class Number extends Arg {
        private double value;

        public Number(double v) {
            value = v;
        }
    }

    public static class Symbol extends Arg {
        private int slot;

        public Symbol(int slot) {
            this.slot = slot;
        }

        public int getSlot() {
            return slot;
        }
    }
}
