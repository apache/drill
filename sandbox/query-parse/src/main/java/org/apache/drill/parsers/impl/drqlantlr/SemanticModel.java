package org.apache.drill.parsers.impl.drqlantlr;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.parsers.DrqlParser;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader;

public class SemanticModel implements SemanticModelReader {
	static class Symbol implements SemanticModelReader.Symbol {
		Type type;
		String name;
		List<String> components;
		Symbol aliasSymbol;
		Symbol aliasedSymbol;
				
		@Override
		public Type getType() {
			return type;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public List<String> getNameComponents() {
			return Collections.unmodifiableList(components);
		}

		@Override
		public boolean isAlias() {
			return aliasSymbol == null;
		}

		@Override
		public SemanticModelReader.Symbol getAlisasedSymbol() {
			return aliasedSymbol;
		}

		@Override
		public SemanticModelReader.Symbol getAliasSymbol() {
			return aliasSymbol;
		}
	}
	static class ResultColumn 
		implements SemanticModelReader.ResultColumn {
		SemanticModelReader.ResultColumn.Scope scope;
		Symbol columnScope;
		Symbol alias;
		SemanticModelReader.Expression expression;

		@Override
		public SemanticModelReader.Symbol getAlias() {
			return alias;
		}

		@Override
		public SemanticModelReader.Expression getExpression() {
			return expression;
		}

		@Override
		public Scope getScope() {
			return scope;
		}

		@Override
		public SemanticModelReader.Symbol getColumnScope() {
			return columnScope;
		}
			
	}
	static class Expression implements SemanticModelReader.Expression{
		static class BinaryOp 
			implements SemanticModelReader.Expression.BinaryOp {
	
			SemanticModelReader.Expression left;
			SemanticModelReader.Expression right;
			Operators operator;
					
			@Override
			public SemanticModelReader.Expression getLeftExpression() {
				return left;
			}
	
			@Override
			public SemanticModelReader.Expression getRightExpression() {
				return right;
			}
	
			@Override
			public Operators getOperator() {
				return operator;
			}
		
		}
		static class UnaryOp 
			implements SemanticModelReader.Expression.UnaryOp {
	
			SemanticModelReader.Expression expression;
			String operator;
			@Override
			public SemanticModelReader.Expression getExpression() {
				return expression;
			}
	
			@Override
			public String getOperator() {
				return operator;
			}
		
		}
		static class Constant 
			implements SemanticModelReader.Expression.Constant {
			Object value;
			
			@Override
			public Object getValue() {
				return value;
			}
		}
		
		static class Column 
			implements SemanticModelReader.Expression.Column {
			Symbol column;
	
			@Override
			public SemanticModelReader.Symbol getSymbol() {
				return column;
			}
		
		}
		static class Function 
			implements SemanticModelReader.Expression.Function {
			Symbol function;
			List<SemanticModelReader.Expression> args;
			
			Function() {
				args = new LinkedList<SemanticModelReader.Expression>();
			}
			
			@Override
			public SemanticModelReader.Symbol getSymbol() {
				return function;
			}
			@Override
			public List<SemanticModelReader.Expression> getArgs() {
				return args;
			}
		
		}
	}
	public static class JoinOnClause implements SemanticModelReader.JoinOnClause {

        public static class JoinCondition implements SemanticModelReader.JoinOnClause.JoinCondition {
            SemanticModelReader.Symbol leftSymbol, rightSymbol;

            public SemanticModelReader.Symbol getLeftSymbol() {
                return leftSymbol;
            }

            public SemanticModelReader.Symbol getRightSymbol() {
                return rightSymbol;
            }
        }

		SemanticModelReader.Symbol table;
		List<SemanticModelReader.JoinOnClause.JoinCondition> conditions;

		JoinOnClause() {
			conditions = new LinkedList<SemanticModelReader.JoinOnClause.JoinCondition>();
		}
		
		@Override
		public SemanticModelReader.Symbol getTable() {
			return table;
		}

		@Override
		public List<SemanticModelReader.JoinOnClause.JoinCondition> getJoinConditionClause() {
			return Collections.unmodifiableList(conditions);
		}
		
	}

	List<SemanticModelReader.Symbol> orderByClause;
	List<SemanticModelReader> fromClause;
	List<SemanticModelReader.ResultColumn> resultColumnList;
	List<SemanticModelReader.Symbol> groupByClause;
	SemanticModelReader.JoinOnClause joinOnClause;
	SemanticModelReader.Expression whereClause;
	SemanticModelReader.Expression havingClause;
	Integer limitClause;
	SemanticModelReader.Symbol justATable;

	public SemanticModel() {
		orderByClause = new LinkedList<SemanticModelReader.Symbol>();
		fromClause = new LinkedList<SemanticModelReader>();
		resultColumnList = new LinkedList<SemanticModelReader.ResultColumn>();
		groupByClause = new LinkedList<SemanticModelReader.Symbol>();
	}
	
	@Override
	public List<SemanticModelReader> getFromClause() {
		return Collections.unmodifiableList(fromClause);
	}

	@Override
	public List<SemanticModelReader.ResultColumn> getResultColumnList() {
		return Collections.unmodifiableList(resultColumnList);
	}

	@Override
	public List<SemanticModelReader.Symbol> getGroupByClause() {
		return Collections.unmodifiableList(groupByClause);
	}

	@Override
	public List<SemanticModelReader.Symbol> getOrderByClause() {
		return Collections.unmodifiableList(orderByClause);
	}

	@Override
	public SemanticModelReader.JoinOnClause getJoinOnClause() {
		return joinOnClause;
	}

	@Override
	public SemanticModelReader.Expression getWhereClause() {
		return whereClause;
	}

	@Override
	public Integer getLimitClause() {
		return limitClause;
	}

	@Override
	public boolean isJustATable() {
		return justATable != null;
	}

	@Override
	public SemanticModelReader.Symbol getjustATable() {
		return justATable;
	}
}