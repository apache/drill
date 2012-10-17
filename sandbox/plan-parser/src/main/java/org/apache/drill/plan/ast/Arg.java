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

    public static Arg createLong(String s) {
        return new Number(Long.parseLong(s.substring(2), 16));
    }

    public static Arg createBoolean(String b) {
        return new BooleanConstant(Boolean.parseBoolean(b));
    }

    public Symbol asSymbol() {
        return (Symbol) this;
    }

    public String asString() {
        return ((QuotedString) this).s;
    }

    public static class QuotedString extends Arg {
        private String s;

        public QuotedString(String s) {
            this.s = quotes.trimFrom(quotes.trimFrom(s));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof QuotedString)) return false;

            QuotedString that = (QuotedString) o;

            return !(s != null ? !s.equals(that.s) : that.s != null);

        }

        @Override
        public int hashCode() {
            return s != null ? s.hashCode() : 0;
        }
    }

    public static class BooleanConstant extends Arg {
        private boolean v;

        public BooleanConstant(boolean b) {
            v = b;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof BooleanConstant && v == ((BooleanConstant) o).v;
        }

        @Override
        public int hashCode() {
            return (v ? 1 : 0);
        }
    }

    public static class Number extends Arg {
        private double value;

        public Number(double v) {
            value = v;
        }

        public double doubleValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof Number && Double.compare(((Number) o).value, value) == 0;
        }

        @Override
        public int hashCode() {
            long temp = value != +0.0d ? Double.doubleToLongBits(value) : 0L;
            return (int) (temp ^ (temp >>> 32));
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

        public int getInt() {
            return slot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            return o instanceof Symbol && slot == ((Symbol) o).slot;
        }

        @Override
        public int hashCode() {
            return slot;
        }
    }
}
