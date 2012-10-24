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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.drill.parsers.impl.drqlantlr.Parser;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.Expression;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.Expression.BinaryOp;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.Expression.BinaryOp.Operators;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.Expression.Column;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.Expression.Function;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.ResultColumn;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.ResultColumn.Scope;
import org.apache.drill.parsers.DrqlParser.SemanticModelReader.Symbol;
import org.junit.Test;

public class DrqlParserTest {

	File getFile(String filename) {
		return new File("testdata" + File.separatorChar + filename);
	}
	void testBasicSymbol(Symbol sym, String name) {
		assertTrue(sym.getAliasSymbol() == null);
		String name2 = sym.getName();
		assertTrue(name2.equals(name));	
	}
	void testBasicResultColumn(ResultColumn col, String name) {
		assertTrue(col.getScope() == Scope.FULL);
		assertTrue(col.getAlias() == null);
		assertTrue(col.getColumnScope() == null);
		Expression expr = col.getExpression();
		assertTrue(expr instanceof Expression.Column);
		Expression.Column colExpr = (Expression.Column) expr;
		Symbol sym = colExpr.getSymbol();
		testBasicSymbol(sym, name);
	}
	void testBasicTable(SemanticModelReader subQuery, String name) {
		assertTrue(subQuery.isJustATable());
		Symbol sym = subQuery.getjustATable();
		testBasicSymbol(sym, name);
	}
	
	@Test
	public void testBasicQuery() throws IOException {
		
		String drqlQueryText = "SELECT column1 FROM table1";
		DrqlParser parser = new Parser();
		SemanticModelReader query = parser.parse(drqlQueryText);

		//result column list
		List<ResultColumn> resColList = query.getResultColumnList();
		assertTrue(resColList.size() == 1);
		ResultColumn col = resColList.get(0);
		testBasicResultColumn(col, "column1");
		
		//from clause
		List<SemanticModelReader> subQueryList = query.getFromClause();
		assertTrue(subQueryList.size() == 1);
		SemanticModelReader subQuery = subQueryList.get(0);
		testBasicTable(subQuery, "table1");
		
		//check the rest of the model
		assertTrue(query.getGroupByClause().size() == 0);
		assertTrue(query.getJoinOnClause() == null);
		assertTrue(query.getOrderByClause().size() == 0);
		assertTrue(query.getLimitClause() == null);
	}
	
	void testAlias(ResultColumn col, String name) {
		Symbol alias = col.getAlias();
		assertTrue(alias.getName().equals(name));
		assertTrue(alias.getType().name().equals("COLUMN_ALIAS"));
	}
	
	@Test
	public void testQuery1() throws IOException {
		
		String drqlQueryText = "SELECT column1 as col1, column2 FROM table1 WHERE col1 > 55 ORDER BY column2 ASC";
		DrqlParser parser = new Parser();
		SemanticModelReader query = parser.parse(drqlQueryText);

		//result column list
		List<ResultColumn> resColList = query.getResultColumnList();
		assertTrue(resColList.size() == 2);
		ResultColumn col1 = resColList.get(0);
		assertTrue(col1.getScope() == Scope.FULL);
		testAlias(col1, "col1");
		assertTrue(col1.getColumnScope() == null);
		Expression expr = col1.getExpression();
		assertTrue(expr instanceof Expression.Column);
		ResultColumn col2 = resColList.get(1);
		testBasicResultColumn(col2, "column2");
		
		//from clause
		List<SemanticModelReader> subQueryList = query.getFromClause();
		assertTrue(subQueryList.size() == 1);
		SemanticModelReader subQuery = subQueryList.get(0);
		testBasicTable(subQuery, "table1");
		
		//where clause
		Expression whereExpr = query.getWhereClause();
		assertTrue(whereExpr instanceof Expression.BinaryOp);
		Expression.BinaryOp whereExpr2 = (BinaryOp) query.getWhereClause();
		assertTrue(whereExpr2.getOperator() == Operators.GREATER_THAN);
		
		//order by clause
		List<Symbol> orderByClause = query.getOrderByClause();
		assertTrue(orderByClause.size() == 1);
		Symbol orderBy1 = orderByClause.get(0);
		assertTrue(orderBy1.getName().equals("column2"));
		assertTrue(orderBy1.getType().name().equals("COLUMN"));
		
		//check the rest of the model
		assertTrue(query.getGroupByClause().size() == 0);
		assertTrue(query.getJoinOnClause() == null);
		assertTrue(query.getLimitClause() == null);
	}
	
	@Test
	public void testQuery2() throws IOException {
		
		String drqlQueryText = 
				"SELECT customersTable.id, customersTable.name, ordersTable.id " + 
				"FROM customersTable " +
				"INNER JOIN ordersTable ON customersTable.id = ordersTable.customerId";

		DrqlParser parser = new Parser();
        String ast = Parser.parseToAst(drqlQueryText).toStringTree();
		SemanticModelReader query = parser.parse(drqlQueryText);

        SemanticModelReader.JoinOnClause join = query.getJoinOnClause();
        assertNotNull(join);
        assertTrue("ordersTable".equals(join.getTable().getName()));
        assertTrue(join.getJoinConditionClause().size() == 1);
        assertTrue(join.getJoinConditionClause().get(0).getLeftSymbol().getType() == Symbol.Type.COLUMN);
        assertTrue("customersTable.id".equals(join.getJoinConditionClause().get(0).getLeftSymbol().getName()));
        assertTrue(join.getJoinConditionClause().get(0).getRightSymbol().getType() == Symbol.Type.COLUMN);
        assertTrue("ordersTable.customerId".equals(join.getJoinConditionClause().get(0).getRightSymbol().getName()));

    }
	
	@Test
	public void testQuery3() throws IOException {
		
		String drqlQueryText = "SELECT COUNT(f1) FROM table1";
		DrqlParser parser = new Parser();
		SemanticModelReader query = parser.parse(drqlQueryText);

		//result column list
		List<ResultColumn> resColList = query.getResultColumnList();
		assertTrue(resColList.size() == 1);
		ResultColumn col1 = resColList.get(0);
		assertTrue(col1.getScope() == Scope.FULL);
		assertTrue(col1.getAlias() == null);
		Expression expr = col1.getExpression();
		assertTrue(expr instanceof Expression.Function);
		Expression.Function func = (Function) col1.getExpression();
		assertTrue(func.getSymbol().getName().equals("COUNT"));
		assertTrue(func.getArgs().size() == 1);
		Expression arg1 = (Expression) func.getArgs().get(0);
		assertTrue(arg1 instanceof Expression.Column);
		Expression.Column arg1Column = (Column) arg1;
		assertTrue(arg1Column.getSymbol().getName().equals("f1"));
		assertTrue(col1.getColumnScope() == null);
		
		//from clause
		List<SemanticModelReader> subQueryList = query.getFromClause();
		assertTrue(subQueryList.size() == 1);
		SemanticModelReader subQuery = subQueryList.get(0);
		testBasicTable(subQuery, "table1");
		
		//check the rest of the model
		assertTrue(query.getGroupByClause().size() == 0);
		assertTrue(query.getJoinOnClause() == null);
		assertTrue(query.getLimitClause() == null);
	}
	
	@Test
	public void testQuery4() throws IOException {
		
		String drqlQueryText = "SELECT COUNT(r1.m2.f3) WITHIN r1.m2 AS cnt FROM [Table1];";
		DrqlParser parser = new Parser();
		SemanticModelReader query = parser.parse(drqlQueryText);

		//result column list
		List<ResultColumn> resColList = query.getResultColumnList();
		assertTrue(resColList.size() == 1);
		ResultColumn col1 = resColList.get(0);
		assertTrue(col1.getScope() == Scope.COLUMN);
		testAlias(col1, "cnt");
		Expression expr = col1.getExpression();
		assertTrue(expr instanceof Expression.Function);
		Expression.Function func = (Function) col1.getExpression();
		assertTrue(func.getSymbol().getName().equals("COUNT"));
		assertTrue(func.getArgs().size() == 1);
		Expression arg1 = (Expression) func.getArgs().get(0);
		assertTrue(arg1 instanceof Expression.Column);
		Expression.Column arg1Column = (Column) arg1;
		assertTrue(arg1Column.getSymbol().getName().equals("r1"));
		
		//from clause
		List<SemanticModelReader> subQueryList = query.getFromClause();
		assertTrue(subQueryList.size() == 1);
		SemanticModelReader subQuery = subQueryList.get(0);
		testBasicTable(subQuery, "[Table1]");
		
		//check the rest of the model
		assertTrue(query.getGroupByClause().size() == 0);
		assertTrue(query.getJoinOnClause() == null);
		assertTrue(query.getLimitClause() == null);
	}
	
	@Test
	public void testQuery5() throws IOException {
		
		String drqlQueryText = "SELECT f1, SUM(f2) FROM [Table1] GROUP BY f1 HAVING SUM(f2) > 1000;";
		DrqlParser parser = new Parser();
		SemanticModelReader query = parser.parse(drqlQueryText);

		//result column list
		List<ResultColumn> resColList = query.getResultColumnList();
		assertTrue(resColList.size() == 2);
		
		ResultColumn col1 = resColList.get(0);
		assertTrue(col1.getScope() == Scope.FULL);
		testBasicResultColumn(col1, "f1");
		
		ResultColumn col2 = resColList.get(1);
		Expression expr = col2.getExpression();
		assertTrue(expr instanceof Expression.Function);
		Expression.Function func = (Function) col2.getExpression();
		assertTrue(func.getSymbol().getName().equals("SUM"));
		assertTrue(func.getArgs().size() == 1);
		Expression arg1 = (Expression) func.getArgs().get(0);
		assertTrue(arg1 instanceof Expression.Column);
		Expression.Column arg1Column = (Column) arg1;
		assertTrue(arg1Column.getSymbol().getName().equals("f2"));
		
		//from clause
		List<SemanticModelReader> subQueryList = query.getFromClause();
		assertTrue(subQueryList.size() == 1);
		SemanticModelReader subQuery = subQueryList.get(0);
		testBasicTable(subQuery, "[Table1]");
		
		//group-by clause
		List<Symbol> groupByList = query.getGroupByClause();
		assertTrue(groupByList.size() == 1);
		Symbol groupBy1 = groupByList.get(0);
		assertTrue(groupBy1.getName().equals("f1"));
		assertTrue(groupBy1.getType().name().equals("COLUMN"));
		
		//check the rest of the model
		assertTrue(query.getJoinOnClause() == null);
		assertTrue(query.getLimitClause() == null);
	}
	
	@Test
	public void testQuery6() throws IOException {
		
		String drqlQueryText = "SELECT COUNT(m1.f2) WITHIN RECORD FROM table1;";
		DrqlParser parser = new Parser();
		SemanticModelReader query = parser.parse(drqlQueryText);

		//result column list
		List<ResultColumn> resColList = query.getResultColumnList();
		assertTrue(resColList.size() == 1);
		
		ResultColumn col1 = resColList.get(0);
		assertTrue(col1.getScope() == Scope.RECORD);
		Expression expr = col1.getExpression();
		assertTrue(expr instanceof Expression.Function);
		Expression.Function func = (Function) col1.getExpression();
		assertTrue(func.getSymbol().getName().equals("COUNT"));
		assertTrue(func.getArgs().size() == 1);
		Expression arg1 = (Expression) func.getArgs().get(0);
		assertTrue(arg1 instanceof Expression.Column);
		Expression.Column arg1Column = (Column) arg1;
		assertTrue(arg1Column.getSymbol().getName().equals("m1"));
		
		//from clause
		List<SemanticModelReader> subQueryList = query.getFromClause();
		assertTrue(subQueryList.size() == 1);
		SemanticModelReader subQuery = subQueryList.get(0);
		testBasicTable(subQuery, "table1");
		
		//check the rest of the model
		assertTrue(query.getGroupByClause().size() == 0);
		assertTrue(query.getJoinOnClause() == null);
		assertTrue(query.getLimitClause() == null);
	}
	
	@Test
	public void testQuery7() throws IOException {
		
		String drqlQueryText = "SELECT column1 FROM table1 LIMIT 5;";
		DrqlParser parser = new Parser();
		SemanticModelReader query = parser.parse(drqlQueryText);

		//result column list
		List<ResultColumn> resColList = query.getResultColumnList();
		assertTrue(resColList.size() == 1);
		ResultColumn col = resColList.get(0);
		testBasicResultColumn(col, "column1");
		
		//from clause
		List<SemanticModelReader> subQueryList = query.getFromClause();
		assertTrue(subQueryList.size() == 1);
		SemanticModelReader subQuery = subQueryList.get(0);
		testBasicTable(subQuery, "table1");
		
		//limit clause
		int limitClause = query.getLimitClause();
		assertTrue(limitClause == 5);
		
		//check the rest of the model
		assertTrue(query.getGroupByClause().size() == 0);
		assertTrue(query.getJoinOnClause() == null);
		assertTrue(query.getOrderByClause().size() == 0);
	}
	
	void testColumnBinaryOpC1(ResultColumn col, Operators op) {
		assertTrue(col.getScope() == Scope.FULL);
		assertTrue(col.getAlias() == null);
		assertTrue(col.getColumnScope() == null);
		Expression expr = col.getExpression();
		assertTrue(expr instanceof Expression.BinaryOp);
		Expression.BinaryOp colExpr = (Expression.BinaryOp) expr;
		assertTrue(colExpr.getOperator() == op);
		Expression left = colExpr.getLeftExpression();
		Expression right = colExpr.getRightExpression();
		assertTrue(left instanceof Expression.Column);
		assertTrue(right instanceof Expression.Constant);
		Expression.Column leftExpr = (Expression.Column) left;
		Expression.Constant rightExpr = (Expression.Constant) right;
		Symbol sym = leftExpr.getSymbol();
		testBasicSymbol(sym, "c");
		assertTrue((Integer)rightExpr.getValue() == 1);
	}
	
	@Test
	public void testQuery8() throws IOException {
		
		String drqlQueryText = "SELECT c - 1, c + 1, c / 1, c * 1 FROM table1;";
		DrqlParser parser = new Parser();
		SemanticModelReader query = parser.parse(drqlQueryText);

		//result column list
		List<ResultColumn> resColList = query.getResultColumnList();
		assertTrue(resColList.size() == 4);
		ResultColumn col1 = resColList.get(0);
		testColumnBinaryOpC1(col1, Operators.SUBTRACT);
		ResultColumn col2 = resColList.get(1);
		testColumnBinaryOpC1(col2, Operators.ADD);
		ResultColumn col3 = resColList.get(2);
		testColumnBinaryOpC1(col3, Operators.DIVIDE);
		ResultColumn col4 = resColList.get(3);
		testColumnBinaryOpC1(col4, Operators.MULTIPLY);
		
		//from clause
		List<SemanticModelReader> subQueryList = query.getFromClause();
		assertTrue(subQueryList.size() == 1);
		SemanticModelReader subQuery = subQueryList.get(0);
		testBasicTable(subQuery, "table1");
		
		//check the rest of the model
		assertTrue(query.getLimitClause() == null);
		assertTrue(query.getGroupByClause().size() == 0);
		assertTrue(query.getJoinOnClause() == null);
		assertTrue(query.getOrderByClause().size() == 0);
	}
	
	@Test
	public void testQuery9() throws IOException {
		
		String drqlQueryText = "SELECT c - 1 + 2 FROM table1;";
		DrqlParser parser = new Parser();
		SemanticModelReader query = parser.parse(drqlQueryText);

		//result column list
		List<ResultColumn> resColList = query.getResultColumnList();
		assertTrue(resColList.size() == 1);
		ResultColumn col1 = resColList.get(0);
		assertTrue(col1.getScope() == Scope.FULL);
		assertTrue(col1.getAlias() == null);
		assertTrue(col1.getColumnScope() == null);
		Expression expr = col1.getExpression();
		assertTrue(expr instanceof Expression.BinaryOp);
		Expression.BinaryOp colExpr = (Expression.BinaryOp) expr;
		assertTrue(colExpr.getOperator() == Operators.ADD);
		Expression left1 = colExpr.getLeftExpression();
		Expression right1 = colExpr.getRightExpression();
		assertTrue(left1 instanceof Expression.BinaryOp);
		assertTrue(right1 instanceof Expression.Constant);
		Expression.BinaryOp leftExpr1 = (Expression.BinaryOp) left1;
		Expression.Constant rightExpr1 = (Expression.Constant) right1;
		assertTrue((Integer)rightExpr1.getValue() == 2);
		assertTrue(leftExpr1.getOperator() == Operators.SUBTRACT);
		Expression left1_2 = leftExpr1.getLeftExpression();
		Expression right1_2 = leftExpr1.getRightExpression();
		Expression.Column leftExpr1_2 = (Expression.Column) left1_2;
		Expression.Constant rightExpr1_2 = (Expression.Constant) right1_2;
		Symbol sym = leftExpr1_2.getSymbol();
		testBasicSymbol(sym, "c");
		assertTrue((Integer)rightExpr1_2.getValue() == 1);
		
		//from clause
		List<SemanticModelReader> subQueryList = query.getFromClause();
		assertTrue(subQueryList.size() == 1);
		SemanticModelReader subQuery = subQueryList.get(0);
		testBasicTable(subQuery, "table1");
		
		//check the rest of the model
		assertTrue(query.getLimitClause() == null);
		assertTrue(query.getGroupByClause().size() == 0);
		assertTrue(query.getJoinOnClause() == null);
		assertTrue(query.getOrderByClause().size() == 0);
	}
	
	@Test
	public void testSyntaxErrorQuery1() throws IOException {
		
		String drqlQueryText = "blah blah blah";
		
		// TODO: provide syntax error messages in g file. write this test.
		
		/*
		DrqlParser parser = new Parser();
		SemanticModelReader query = parser.parse(drqlQueryText);
		
		
		//check the rest of the model
		assertTrue(query.getResultColumnList().size() == 0);
		assertTrue(query.getFromClause().size() == 0);
		assertTrue(query.getGroupByClause().size() == 0);
		assertTrue(query.getJoinOnClause() == null);
		assertTrue(query.getLimitClause() == null);
		assertTrue(query.getOrderByClause().size() == 0);
		*/
	}
}
