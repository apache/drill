package org.apache.drill.common.expression;


public class ValueExpressions {

	protected static abstract class ValueExpression<V> extends
			LogicalExpressionBase {
		public final V value;

		protected ValueExpression(String value) {
			this.value = parseValue(value);
		}

		protected abstract V parseValue(String s);


	}

	public static class BooleanExpression extends ValueExpression<Boolean> {
		public BooleanExpression(String value) {
			super(value);
		}

		@Override
		protected Boolean parseValue(String s) {
			return Boolean.parseBoolean(s);
		}

    @Override
    public void addToString(StringBuilder sb) {
      sb.append(value.toString());
    }
		
	}

	public static class NumberExpression extends ValueExpression<Number> {
		public NumberExpression(String value) {
			super(value);
		}

		@Override
		protected Number parseValue(String s) {
			return Integer.parseInt(s);
		}
		
    @Override
    public void addToString(StringBuilder sb) {
      sb.append(value.toString());
    }
	}

	public static class QuotedString extends ValueExpression<String> {
		public QuotedString(String value) {
			super(value);
		}

		@Override
		protected String parseValue(String s) {
			return s;
		}
		
    @Override
    public void addToString(StringBuilder sb) {
      sb.append("\"");
      sb.append(value.toString());
      sb.append("\"");
    }
	}

	public static class Identifier extends ValueExpression<String> {
		public Identifier(String value) {
			super(value);
		}

		@Override
		protected String parseValue(String s) {
			return s;
		}
		
    @Override
    public void addToString(StringBuilder sb) {
      sb.append("'");
      sb.append(value.toString());
      sb.append("'");
    }
	}
}
