parser grammar ExprParser;

options{
  output=AST;
  language=Java;
  tokenVocab=ExprLexer;
  backtrack=true;
  memoize=true;
}



@header {
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.common.expression.parser;
  
//Explicit import...
import org.antlr.runtime.BitSet;
import java.util.*;
import org.apache.drill.common.expression.*;

}

@members{
  private FunctionRegistry registry;
  private String fullExpression;
  private int tokenPos;
  public void setRegistry(FunctionRegistry registry){
    this.registry = registry;
  }

  public static void p(String s){
    System.out.println(s);
  }
  
  public ExpressionPosition pos(Token token){
    return new ExpressionPosition(fullExpression, token.getTokenIndex());
  }
}

parse returns [LogicalExpression e]
  :  expression EOF {
    $e = $expression.e; 
    if(fullExpression == null) fullExpression = $expression.text;
    tokenPos = $expression.start.getTokenIndex();
  }
  ;
 
functionCall returns [LogicalExpression e]
  :  Identifier OParen exprList? CParen {$e = registry.createExpression($Identifier.text, pos($Identifier), $exprList.listE);  }
  ;

ifStatement returns [LogicalExpression e]
	@init {
	  IfExpression.Builder s = IfExpression.newBuilder();
	}
	@after {
	  $e = s.build();
	}  
  :  i1=ifStat {s.addCondition($i1.i); s.setPosition(pos($i1.start)); } (elseIfStat { s.addCondition($elseIfStat.i); } )* Else expression { s.setElse($expression.e); }End 
  ;

ifStat returns [IfExpression.IfCondition i]
  : If e1=expression Then e2=expression { $i = new IfExpression.IfCondition($e1.e, $e2.e); }
  ;
elseIfStat returns [IfExpression.IfCondition i]
  : Else If e1=expression Then e2=expression { $i = new IfExpression.IfCondition($e1.e, $e2.e); }
  ;

caseStatement returns [LogicalExpression e]
	@init {
	  IfExpression.Builder s = IfExpression.newBuilder();
	}
	@after {
	  $e = s.build();
	}  
  : Case (caseWhenStat {s.addCondition($caseWhenStat.i); }) + caseElseStat { s.setElse($caseElseStat.e); } End 
  ;
  
caseWhenStat returns [IfExpression.IfCondition i]
  : When e1=expression Then e2=expression {$i = new IfExpression.IfCondition($e1.e, $e2.e); }
  ;
  
caseElseStat returns [LogicalExpression e]
  : Else expression {$e = $expression.e; }
  ;
  
exprList returns [List<LogicalExpression> listE]
	@init{
	  $listE = new ArrayList<LogicalExpression>();
	}
  :  e1=expression {$listE.add($e1.e); } (Comma e2=expression {$listE.add($e2.e); } )*
  ;

expression returns [LogicalExpression e]  
  :  ifStatement {$e = $ifStatement.e; }
  |  caseStatement {$e = $caseStatement.e; }
  |  condExpr {$e = $condExpr.e; }
  ;

condExpr returns [LogicalExpression e]
  :  orExpr {$e = $orExpr.e; }
  ;

orExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	  ExpressionPosition p = null;
	}
	@after{
	  if(exprs.size() == 1){
	    $e = exprs.get(0);
	  }else{
	    $e = registry.createExpression("||", p, exprs);
	  }
	}
  :  a1=andExpr { exprs.add($a1.e); p = pos( $a1.start );} (Or^ a2=andExpr { exprs.add($a2.e); })*
  ;

andExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	  ExpressionPosition p = null;
	}
	@after{
	  if(exprs.size() == 1){
	    $e = exprs.get(0);
	  }else{
	    $e = registry.createExpression("&&", p, exprs);
	  }
	}
  :  e1=equExpr { exprs.add($e1.e); p = pos( $e1.start );  } (And^ e2=equExpr { exprs.add($e2.e);  })*
  ;

equExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	  List<String> cmps = new ArrayList();
	  ExpressionPosition p = null;
	}
	@after{
	  $e = registry.createByOp(exprs, p, cmps);
	}
  :  r1=relExpr { exprs.add($r1.e); p = pos( $r1.start );
    } ( cmpr= (Equals | NEquals ) r2=relExpr {exprs.add($r2.e); cmps.add($cmpr.text); })*
  ;

relExpr returns [LogicalExpression e]
  :  left=addExpr {$e = $left.e; } (cmpr = (GTEquals | LTEquals | GT | LT) right=addExpr {$e = registry.createExpression($cmpr.text, pos($left.start), $left.e, $right.e); } )? 
  ;

addExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	  List<String> ops = new ArrayList();
	  ExpressionPosition p = null;
	}
	@after{
	  $e = registry.createByOp(exprs, p, ops);
	}
  :  m1=mulExpr  {exprs.add($m1.e); p = pos($m1.start); } ( op=(Plus|Minus) m2=mulExpr {exprs.add($m2.e); ops.add($op.text); })* 
  ;

mulExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	  List<String> ops = new ArrayList();
	  ExpressionPosition p = null;
	}
	@after{
	  $e = registry.createByOp(exprs, p, ops);
	}
  :  p1=powExpr  {exprs.add($p1.e); p = pos($p1.start);} (op=(Asterisk|ForwardSlash|Percent) p2=powExpr {exprs.add($p2.e); ops.add($op.text); } )*
  ;

powExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	  List<String> ops = new ArrayList();
	  ExpressionPosition p = null;
	}
	@after{
	  $e = registry.createByOp(exprs, p, ops);
	}
  :  u1=unaryExpr {exprs.add($u1.e); p = pos($u1.start);} (Caret u2=unaryExpr {exprs.add($u2.e); ops.add($Caret.text);} )*
  ;
  
unaryExpr returns [LogicalExpression e]
  :  Minus atom {$e = registry.createExpression("u-", pos($atom.start), $atom.e); }
  |  Excl atom {$e= registry.createExpression("!", pos($atom.start), $atom.e); }
  |  atom {$e = $atom.e; }
  ;

atom returns [LogicalExpression e]
  :  Number {$e = ValueExpressions.getNumericExpression($Number.text, pos($atom.start)); }
  |  Bool {$e = new ValueExpressions.BooleanExpression( $Bool.text, pos($atom.start)); }
  |  lookup {$e = $lookup.e; }
  ;


lookup returns [LogicalExpression e]
  :  functionCall {$e = $functionCall.e ;}
  | Identifier {$e = new SchemaPath($Identifier.text, pos($Identifier) ); }
  | String {$e = new ValueExpressions.QuotedString($String.text, pos($String) ); }
  | OParen expression CParen  {$e = $expression.e; }
  | SingleQuote Identifier SingleQuote {$e = new SchemaPath($Identifier.text, pos($Identifier) ); }
  ;
