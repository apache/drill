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
package org.apache.drill.parsers.impl.drqlantlr;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.Tree;
import org.antlr.runtime.tree.TreeAdaptor;

import org.apache.drill.parsers.DrqlParser;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.Expression;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.Expression.BinaryOp.Operators;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.Symbol.Type;
import org.apache.drill.parsers.impl.drqlantlr.SemanticModel.Symbol;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;

public class AntlrParser implements DrqlParser{
	/**
	 * ANTLRNoCaseStringStream. //This class provides and implementation for a
	 * case insensitive token checker //for the lexical analysis part of antlr.
	 * By converting the token stream into //upper case at the time when lexical
	 * rules are checked, this class ensures that the //lexical rules need to
	 * just match the token with upper case letters as opposed to //combination
	 * of upper case and lower case characters. This is purely used for matching
	 * lexical //rules. The actual token text is stored in the same way as the
	 * user input without //actually converting it into an upper case. The token
	 * values are generated by the consume() //function of the super class
	 * ANTLRStringStream. The LA() function is the lookahead function //and is
	 * purely used for matching lexical rules. This also means that the grammar
	 * will only //accept capitalized tokens in case it is run from other tools
	 * like antlrworks which //do not have the ANTLRNoCaseStringStream
	 * implementation.
	 */
	static class ANTLRNoCaseStringStream extends ANTLRStringStream {

		public ANTLRNoCaseStringStream(String input) {
			super(input);
		}

		@Override
		public int LA(int i) {

			int returnChar = super.LA(i);
			if (returnChar == CharStream.EOF) {
				return returnChar;
			} else if (returnChar == 0) {
				return returnChar;
			}

			return Character.toUpperCase((char) returnChar);
		}
	}
	static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
		@Override
		public Object create(Token payload) {
			return new AstNode(payload);
		}
	};
	@Override
	public SemanticModel parse(String drqlQueryText) {
    	AstNode node = parseToAst(drqlQueryText);
    	SemanticModel query = 
    			new SemanticModel();
    	parseSelectStatement(node, query);
    	return query;
    }
	public static AstNode parseToAst(String drQl) {
    	try {
    		DrqlAntlrLexer lexer = new DrqlAntlrLexer(new ANTLRNoCaseStringStream(drQl));
			TokenRewriteStream tokens = new TokenRewriteStream(lexer);
			DrqlAntlrParser parser = new DrqlAntlrParser(tokens);
			parser.setTreeAdaptor(adaptor);
			return (AstNode) parser.selectStatement().getTree();
    	} catch(Exception ex) {
    		throw new RuntimeException("Parsing of the query "+drQl + " has failed", ex);
    	}
	}
	static void parseSelectStatement(AstNode node, SemanticModel query) {
		assert (node.getType() == DrqlAntlrParser.N_SELECT_STATEMENT);
		int count = node.getChildCount();
		assert (count >= 2);
		parseFromClause((AstNode) node.getChild(0), query);
		parseSelectClause((AstNode) node.getChild(1), query);
		int curNode = 2;
		if (node.getChild(curNode) != null && node.getChild(curNode).getType() == DrqlAntlrParser.N_JOIN) {
			parseJoinClause((AstNode) node.getChild(curNode), query);
			curNode++;
		}
		if (node.getChild(curNode) != null && node.getChild(curNode).getType() == DrqlAntlrParser.N_WHERE) {
			parseWhereClause((AstNode) node.getChild(curNode), query);
			curNode++;
		}
		if (node.getChild(curNode) != null && node.getChild(curNode).getType() == DrqlAntlrParser.N_GROUPBY) {
			parseGroupByClause((AstNode) node.getChild(curNode), query);
			curNode++;		
		}
		if (node.getChild(curNode) != null && node.getChild(curNode).getType() == DrqlAntlrParser.N_HAVING) {
			parseHavingClause((AstNode) node.getChild(curNode), query);
			curNode++;		
		}
		if (node.getChild(curNode) != null && node.getChild(curNode).getType() == DrqlAntlrParser.N_ORDERBY) {
			parseOrderByClause((AstNode) node.getChild(curNode), query);
			curNode++;		
		}
		if (node.getChild(curNode) != null && node.getChild(curNode).getType() == DrqlAntlrParser.N_LIMIT) {
			parseLimitClause((AstNode) node.getChild(curNode), query);
			curNode++;		
		}
	}
	static boolean parseWithinRecordClause(AstNode node, SemanticModel.ResultColumn column) {
		assert (node.getType() == DrqlAntlrParser.N_WITHIN_RECORD);
		column.scope = DrqlParser.SemanticModelReader.ResultColumn.Scope.RECORD;
		return true;
	}
	static boolean parseWithinClause(AstNode node, SemanticModel.ResultColumn column) {
		assert (node.getType() == DrqlAntlrParser.N_WITHIN);
		column.scope = DrqlParser.SemanticModelReader.ResultColumn.Scope.COLUMN;
		column.columnScope = new Symbol();
		//column.columnScope.name = TODO add column id from AST 
		return true;
	}
	static void parseColumnAlias(AstNode node, SemanticModel.ResultColumn column) {
		assert (node.getType() == DrqlAntlrParser.N_ALIAS);
		int count = node.getChildCount();
		assert ((count == 1));
		column.alias = new Symbol();
		column.alias.name = node.getChild(0).toString();
		column.alias.type = DrqlParser.SemanticModelReader.Symbol.Type.COLUMN_ALIAS;
	}
	static String idNode2String(Tree idNode) {
		StringBuilder ret = new StringBuilder();
		assert (idNode.getType() == DrqlAntlrParser.N_ID);
		assert (idNode.getChildCount() >= 1);
		AstNode nameNode = (AstNode) idNode.getChild(0);
		assert (nameNode.getType() == DrqlAntlrParser.N_NAME);
		assert (nameNode.getChildCount() == 1);
		ret.append(nameNode.getChild(0).toString());

		for (int i = 1; i < idNode.getChildCount(); i++) {
			nameNode = (AstNode) idNode.getChild(i);
			assert (nameNode.getType() == DrqlAntlrParser.N_NAME);
			assert (nameNode.getChildCount() == 1);
			ret.append(".");
			ret.append(nameNode.getChild(0).toString());
		}
		return ret.toString();
	}
	static String idNode2String(AstNode idNode) {
		StringBuilder ret = new StringBuilder();
		assert (idNode.getType() == DrqlAntlrParser.N_ID);
		assert (idNode.getChildCount() >= 1);
		AstNode nameNode = (AstNode) idNode.getChild(0);
		assert (nameNode.getType() == DrqlAntlrParser.N_NAME);
		assert (nameNode.getChildCount() == 1);
		ret.append(nameNode.getChild(0).toString());

		for (int i = 1; i < idNode.getChildCount(); i++) {
			nameNode = (AstNode) idNode.getChild(i);
			assert (nameNode.getType() == DrqlAntlrParser.N_NAME);
			assert (nameNode.getChildCount() == 1);
			ret.append(".");
			ret.append(nameNode.getChild(0).toString());
		}
		return ret.toString();
	}
	static void parseSelectClauseColumn(AstNode node, SemanticModel query) {
		SemanticModel.ResultColumn column = new SemanticModel.ResultColumn();
		column.scope = DrqlParser.SemanticModelReader.ResultColumn.Scope.FULL;
		assert (node.getType() == DrqlAntlrParser.N_COLUMN);
		assert (node.getChild(0).getType() == DrqlAntlrParser.N_EXPRESSION);
		int count = node.getChildCount();
		assert ((count >= 1) && (count <= 3));
		if (count == 3) {
			parseColumnAlias((AstNode) node.getChild(1), column);
			if (node.getChild(2).getType() == DrqlAntlrParser.N_WITHIN)
				parseWithinClause((AstNode) node.getChild(2), column);
			else if (node.getChild(2).getType() == DrqlAntlrParser.N_WITHIN_RECORD)
				parseWithinRecordClause((AstNode) node.getChild(2), column);
		} else if (count == 2) {
			if (node.getChild(1).getType() == DrqlAntlrParser.N_ALIAS) {
				parseColumnAlias((AstNode) node.getChild(1), column);
			} else if (node.getChild(1).getType() == DrqlAntlrParser.N_WITHIN)
				parseWithinClause((AstNode) node.getChild(1), column);
			else if (node.getChild(1).getType() == DrqlAntlrParser.N_WITHIN_RECORD)
				parseWithinRecordClause(
						(AstNode) node.getChild(1), column);
			else {
				assert (false);
			}
		}

		DrqlParser.SemanticModelReader.Expression expression = 
				createExpression((AstNode) node.getChild(0));
		column.expression = expression;
		query.resultColumnList.add(column);
	};
	private static void parseSelectClause(AstNode node, SemanticModel query) {
		assert (node.getType() == DrqlAntlrParser.N_SELECT);
		int columnCount = node.getChildCount();
		assert (columnCount > 0);
		for (int i = 0; i < columnCount; i++) {
			parseSelectClauseColumn((AstNode) node.getChild(i), query);
		}
	}
	/*
			AstNode culmnNode = (AstNode) ;
			assert (culmnNode.getType() == DrqlAntlrParser.N_COLUMN);
			AstNode expressionNode = (AstNode) node.getChild(0);
			assert (expressionNode.getType() == DrqlAntlrParser.N_EXPRESSION);
			AstNode idNode = (AstNode) node.getChild(0);
			assert (idNode.getType() == DrqlAntlrParser.N_ID);
			int namesCount = idNode.getChildCount();
			assert (namesCount >= 1);
			for (int j = 0; j < namesCount; j++) {
				AstNode nameNode = (AstNode) node.getChild(j);
				assert (nameNode.getType() == DrqlAntlrParser.N_NAME);
				SemanticModel.Symbol column = new SemanticModel.Symbol();
				column.name = curNode.getText();
				column.type = DrqlParser.SemanticModelReader.Symbol.Type.COLUMN;
			}
				query.resultColumnList.add(expression);
				AstNode node3 = (AstNode) node2.getChild(j);
				if (node3.getType() == DrqlAntlrParser.N_COLUMN) {
					parseWithinClause(node3, query);
				}
			}
		}
	}*/
	
	
	private static void parseFromClause(AstNode node, SemanticModel query) {
		assert (node.getType() == DrqlAntlrParser.N_FROM);
		int count = node.getChildCount();
		assert (count > 0);
		for (int i = 0; i < count; i++) {
			AstNode node2 = (AstNode) node.getChild(i);
			if (node2.getType() == DrqlAntlrParser.N_TABLE_NAME) {
				assert (node2.getChildCount() == 1);
				SemanticModel subQuery = new SemanticModel();
				SemanticModel.Symbol table = new SemanticModel.Symbol();
				table.name = node2.getChild(0).getText();
				table.type = DrqlParser.SemanticModelReader.Symbol.Type.TABLE;
				subQuery.justATable = table;
				query.fromClause.add(subQuery);
			} else if (node2.getType() == DrqlAntlrParser.N_SELECT_STATEMENT) {
				assert (node2.getChildCount() == 1);
				SemanticModel subQuery = new SemanticModel();
				parseSelectStatement(node2, subQuery);
				query.fromClause.add(subQuery);
			} else
				assert (false);
		}		
	}
	private static void parseGroupByClause(AstNode node, SemanticModel query) {
		assert (node.getType() == DrqlAntlrParser.N_GROUPBY);	
		int count = node.getChildCount();
		assert (count > 0);
		for (int i = 0; i < count; i++) {
			AstNode node2 = (AstNode) node.getChild(i);		
			assert (node2.getChildCount() == 1);
			SemanticModel.Symbol column = new SemanticModel.Symbol();
			column.name = node2.getChild(0).getText();
			column.type = DrqlParser.SemanticModelReader.Symbol.Type.COLUMN;
			query.groupByClause.add(column);
		}	
	}

    private static void parseWhereClause(AstNode node, SemanticModel query) {
		if (node == null) {
			return;
		}
		assert (node.getType() == DrqlAntlrParser.N_WHERE);
		int count = node.getChildCount();
		assert (count == 1);
		AstNode node2 = (AstNode) node.getChild(0);
		assert (node2.getType() == DrqlAntlrParser.N_EXPRESSION);
		Tree node3 = node2.getChild(0);
		query.whereClause = createExpression(node3);
	}

	static Expression.Function parseFunction(AstNode node) {
		SemanticModel.Expression.Function func = new SemanticModel.Expression.Function();
		func.function = new SemanticModel.Symbol();
		
		String funcName = null;
		List<Expression> args = new ArrayList<Expression>();
		AstNode n2, n3;
		
		for (int i = 0; i < node.getChildCount(); i++) {
			n2 = (AstNode) node.getChild(i);
			if (n2.getType() == DrqlAntlrParser.N_EXPRESSION) {
				n3 = (AstNode) n2.getChild(0);
				assert(n3.getType() == DrqlAntlrParser.N_ID);
				args.add(createExpression(n3));
			} else { 
				assert(n2.getType() == DrqlAntlrParser.N_ID); 
				n3 = (AstNode) n2.getChild(0);
				assert(n3.getType() == DrqlAntlrParser.N_NAME);
				funcName = n3.getChild(0).getText();
			}
		}
		func.function.type = Type.FUNCTION;
		func.function.name = funcName;
		func.args = args;
		return func;
	}
	static Expression.Function parseFunction(Tree node) {
		SemanticModel.Expression.Function func = new SemanticModel.Expression.Function();
		func.function = new SemanticModel.Symbol();

		String funcName = null;
		List<Expression> args = new ArrayList<Expression>();
		AstNode n2, n3;

		for (int i = 0; i < node.getChildCount(); i++) {
			n2 = (AstNode) node.getChild(i);
			if (n2.getType() == DrqlAntlrParser.N_EXPRESSION) {
				n3 = (AstNode) n2.getChild(0);
				assert(n3.getType() == DrqlAntlrParser.N_ID);
				args.add(createExpression(n3));
			} else {
				assert(n2.getType() == DrqlAntlrParser.N_ID);
				n3 = (AstNode) n2.getChild(0);
				assert(n3.getType() == DrqlAntlrParser.N_NAME);
				funcName = n3.getChild(0).getText();
			}
		}
		func.function.type = Type.FUNCTION;
		func.function.name = funcName;
		func.args = args;
		return func;
	}

	static SemanticModelReader.Expression createExpression(Tree node) {
		SemanticModelReader.Expression result = null;
		SemanticModel.Expression.BinaryOp binaryOp;
		//AstNode n2, n3;
        Tree n2, n3;
		SemanticModel.Expression.Column column;
		SemanticModel.Expression.Constant constant;
		
		switch (node.getType()) {
		
		case DrqlAntlrParser.N_LOGICAL_AND:
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.AND;
			result = binaryOp;
			break;
		
		case DrqlAntlrParser.N_LOGICAL_OR:
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.OR;
			result = binaryOp;
			break;
			
		case DrqlAntlrParser.N_CONTAINS:	
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.CONTAINS;
			result = binaryOp;
			break;
			
		case DrqlAntlrParser.N_GREATER_THAN:	
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.GREATER_THAN;
			result = binaryOp;
			break;
			
		case DrqlAntlrParser.N_GREATER_THAN_OR_EQUAL:	
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.GREATER_THAN_OR_EQUAL;
			result = binaryOp;
			break;
			
		case DrqlAntlrParser.EQUAL:	
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.EQUAL;
			result = binaryOp;
			break;
			
		case DrqlAntlrParser.LESS_THAN:	
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.LESS_THAN;
			result = binaryOp;
			break;
			
		case DrqlAntlrParser.LESS_THAN_OR_EQUAL:	
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.LESS_THAN_OR_EQUAL;
			result = binaryOp;
			break;
		
		case DrqlAntlrParser.N_SUBSTRUCT:
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.SUBTRACT;
			result = binaryOp;
			break;
			
		case DrqlAntlrParser.N_ADD:
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.ADD;
			result = binaryOp;
			break;
			
		case DrqlAntlrParser.N_MULTIPLY:
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.MULTIPLY;
			result = binaryOp;
			break;
			
		case DrqlAntlrParser.N_DIVIDE:
			assert (node.getChildCount() == 2);
			binaryOp = new SemanticModel.Expression.BinaryOp();
			binaryOp.left = createExpression(node.getChild(0));
			binaryOp.right = createExpression(node.getChild(1));
			binaryOp.operator = Operators.DIVIDE;
			result = binaryOp;
			break;
			
		case DrqlAntlrParser.N_EXPRESSION:
			assert(node.getChildCount() == 1);
            n2 = node.getChild(0);
			if (n2.getType() == DrqlAntlrParser.N_CALL_PARAMS) {
				result = parseFunction(n2);
			} else if (n2.getType() == DrqlAntlrParser.N_ID) {
				SemanticModel.Expression.Column colExpr = new SemanticModel.Expression.Column();
				colExpr.column = new SemanticModel.Symbol();
				colExpr.column.name = idNode2String(n2);
				result = colExpr;
			} else {
				result = createExpression(n2);
			}
			break;
			
		case DrqlAntlrParser.N_ID:
			n2 = node.getChild(0);
			assert (n2.getType() == DrqlAntlrParser.N_NAME);
			n3 = n2.getChild(0);
			assert (n3.getType() == DrqlAntlrParser.ID);
			column = new SemanticModel.Expression.Column();
			Symbol colSymbol = new Symbol();
			colSymbol.name = n3.getText();
			colSymbol.type = Type.COLUMN;
			column.column = colSymbol;
			result = column;
			break;
		
		case DrqlAntlrParser.N_INT:
			n2 = node.getChild(0);
			assert (n2.getType() == DrqlAntlrParser.INT);
			Integer integer = Integer.parseInt(n2.getText());
			constant = new SemanticModel.Expression.Constant();
			constant.value = integer;
			result = constant;
			break;
		}
		
		return result;
	}
//	static SemanticModelReader.Expression createExpression(AstNode node) {
//		SemanticModelReader.Expression result = null;
//		SemanticModel.Expression.BinaryOp binaryOp;
//		AstNode n2, n3;
//		SemanticModel.Expression.Column column;
//		SemanticModel.Expression.Constant constant;
//
//		switch (node.getType()) {
//
//		case DrqlAntlrParser.N_LOGICAL_AND:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.AND;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.N_LOGICAL_OR:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.OR;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.N_CONTAINS:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.CONTAINS;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.N_GREATER_THAN:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.GREATER_THAN;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.N_GREATER_THAN_OR_EQUAL:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.GREATER_THAN_OR_EQUAL;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.EQUAL:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.EQUAL;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.LESS_THAN:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.LESS_THAN;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.LESS_THAN_OR_EQUAL:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.LESS_THAN_OR_EQUAL;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.N_SUBSTRUCT:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.SUBTRACT;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.N_ADD:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.ADD;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.N_MULTIPLY:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.MULTIPLY;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.N_DIVIDE:
//			assert (node.getChildCount() == 2);
//			binaryOp = new SemanticModel.Expression.BinaryOp();
//			binaryOp.left = createExpression((AstNode) node.getChild(0));
//			binaryOp.right = createExpression((AstNode) node.getChild(1));
//			binaryOp.operator = Operators.DIVIDE;
//			result = binaryOp;
//			break;
//
//		case DrqlAntlrParser.N_EXPRESSION:
//			assert(node.getChildCount() == 1);
//            Tree n2a = node.getChild(0);
//			if (n2a.getType() == DrqlAntlrParser.N_CALL_PARAMS) {
//				result = parseFunction(n2a);
//			} else if (n2a.getType() == DrqlAntlrParser.N_ID) {
//				SemanticModel.Expression.Column colExpr = new SemanticModel.Expression.Column();
//				colExpr.column = new SemanticModel.Symbol();
//				colExpr.column.name = idNode2String(n2a);
//				result = colExpr;
//			} else {
//				result = createExpression(n2a);
//			}
//			break;
//
//		case DrqlAntlrParser.N_ID:
//			n2 = (AstNode) node.getChild(0);
//			assert (n2.getType() == DrqlAntlrParser.N_NAME);
//			n3 = (AstNode) n2.getChild(0);
//			assert (n3.getType() == DrqlAntlrParser.ID);
//			column = new SemanticModel.Expression.Column();
//			Symbol colSymbol = new Symbol();
//			colSymbol.name = n3.getText();
//			colSymbol.type = Type.COLUMN;
//			column.column = colSymbol;
//			result = column;
//			break;
//
//		case DrqlAntlrParser.N_INT:
//			n2 = (AstNode) node.getChild(0);
//			assert (n2.getType() == DrqlAntlrParser.INT);
//			Integer integer = (Integer) Integer.parseInt(n2.getText());
//			constant = new SemanticModel.Expression.Constant();
//			constant.value = integer;
//			result = constant;
//			break;
//		}
//
//		return result;
//	}
	private static void parseLimitClause(AstNode node, SemanticModel query) {
		assert (node.getType() == DrqlAntlrParser.N_LIMIT);
		int count = node.getChildCount();
		assert (count == 1);
		AstNode node2 = (AstNode) node.getChild(0);
		query.limitClause = Integer.parseInt(node2.getText());
	}
	private static void parseOrderByClause(AstNode node, SemanticModel query) {
		assert (node.getType() == DrqlAntlrParser.N_ORDERBY);
		int count = node.getChildCount();
		for (int i = 0; i < count; i ++) {
			AstNode node2 = (AstNode) node.getChild(i);
			assert (node2.getType() == DrqlAntlrParser.N_ASC || 
					node2.getType() == DrqlAntlrParser.N_DESC);
			AstNode node3 = (AstNode) node2.getChild(0);
			assert (node3.getType() == DrqlAntlrParser.N_NAME);
			AstNode node4 = (AstNode) node3.getChild(0);
			String columnName = node4.getText();
			SemanticModel.Symbol column = new SemanticModel.Symbol();
			column.name = columnName;
			column.type = DrqlParser.SemanticModelReader.Symbol.Type.COLUMN;
			query.orderByClause.add(column);	
		}
	}
	private static void parseHavingClause(AstNode node, SemanticModel query) {
		assert (node.getType() == DrqlAntlrParser.N_HAVING);
		int count = node.getChildCount();
		assert (count == 1);
		AstNode node2 = (AstNode) node.getChild(0);
		assert (node2.getType() == DrqlAntlrParser.N_EXPRESSION);
		AstNode node3 = (AstNode) node2.getChild(0);
		query.havingClause = createExpression(node3);
	}
	private static void parseJoinClause(AstNode node, SemanticModel query) {
		assert (node.getType() == DrqlAntlrParser.N_JOIN);
		int count = node.getChildCount();
		assert (count == 3);

        SemanticModel.Symbol table = new SemanticModel.Symbol();

        Tree joinFrom = node.getChild(1);
        if(joinFrom.getType() == DrqlAntlrParser.N_TABLE) {
            table.name = joinFrom.getChild(0).getChild(0).getChild(0).getText();
            table.type = DrqlParser.SemanticModelReader.Symbol.Type.TABLE;
        }


        Tree joinOnList = node.getChild(2);
        ArrayList<SemanticModelReader.JoinOnClause.JoinCondition> joinConditions = new ArrayList<SemanticModelReader.JoinOnClause.JoinCondition>(joinOnList.getChildCount());

        if(joinOnList.getType() == DrqlAntlrParser.N_JOIN_ON_LIST) {
            for(int i=0; i<joinOnList.getChildCount(); i++) {

                SemanticModel.JoinOnClause.JoinCondition joinCondition = new SemanticModel.JoinOnClause.JoinCondition();
                assert(joinOnList.getChild(i).getChildCount() == 2);
                Tree leftNode = joinOnList.getChild(i).getChild(0);
                Tree rightNode = joinOnList.getChild(i).getChild(1);
                SemanticModel.Symbol leftSymbol =  new SemanticModel.Symbol();
                leftSymbol.name = idNode2String((AstNode) leftNode);
                leftSymbol.type = Type.COLUMN;
                SemanticModel.Symbol rightSymbol =  new Symbol();
                rightSymbol.name = idNode2String((AstNode) rightNode);
                rightSymbol.type = Type.COLUMN;

                joinCondition.leftSymbol = leftSymbol;
                joinCondition.rightSymbol = rightSymbol;

                joinConditions.add(joinCondition);
            }
        }

        SemanticModel.JoinOnClause joinOnClause = new SemanticModel.JoinOnClause();
        joinOnClause.table = table;
        joinOnClause.conditions = joinConditions;

        query.joinOnClause = joinOnClause;
	}
}
