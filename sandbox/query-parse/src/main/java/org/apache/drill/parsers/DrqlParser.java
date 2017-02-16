/**
 * Copyright 2010, BigDataCraft.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.parsers;

import java.util.List;

//TODO add java docs
public interface DrqlParser {
	interface SemanticModelReader {
		interface Symbol {
			enum Type {
			    TABLE, COLUMN, COLUMN_ALIAS, FUNCTION, AGG_FUNCTION
			}
			Type getType();
			String getName();
			List<String> getNameComponents();
			boolean isAlias();
			Symbol getAlisasedSymbol();
			Symbol getAliasSymbol();
		}
		interface ResultColumn {
			enum Scope {FULL, RECORD, COLUMN}
			Scope getScope();
			Symbol getColumnScope();
			Symbol getAlias();
			Expression getExpression();
		}
		interface Expression {
				interface BinaryOp extends Expression {
					enum Operators {CONTAINS, GREATER_THAN, GREATER_THAN_OR_EQUAL, 
						LESS_THAN, LESS_THAN_OR_EQUAL, EQUAL, AND, OR, ADD, SUBTRACT,
						MULTIPLY, DIVIDE}
					Expression getLeftExpression();
					Expression getRightExpression();
					Operators getOperator();
				}
	
				interface UnaryOp extends Expression {
					Expression getExpression();
					String getOperator();
				}
	
				interface Constant extends Expression {
					Object getValue();
				}
	
				interface Column extends Expression {
					Symbol getSymbol();
				}
	
				interface Function extends Expression {
					Symbol getSymbol();
					List<Expression> getArgs();
				}
				
		}
		interface JoinOnClause {
			interface JoinCondition {
				Symbol getLeftSymbol();
				Symbol getRightSymbol();
			}
			Symbol getTable();
			List<JoinCondition> getJoinConditionClause();
		}
		
		//Any 'from' clause item is modeled as DrqlQuery
		boolean isJustATable(); 
		Symbol getjustATable(); 
		 
		List<SemanticModelReader> getFromClause(); 
		List<ResultColumn> getResultColumnList();
		List<Symbol> getGroupByClause();
		List<Symbol> getOrderByClause();
		JoinOnClause getJoinOnClause(); 
		Expression getWhereClause(); 
		Integer getLimitClause(); 
	}
	SemanticModelReader parse(String drqlQueryText);
}
