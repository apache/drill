parser grammar ExprParser;

options{
  output=AST;
  language=Java;
  tokenVocab=ExprLexer;
  backtrack=true;
  memoize=true;
}



@header {
  package org.apache.drill.common.expression.parser;
  
  //Explicit import...
  import org.antlr.runtime.BitSet;
  import java.util.*;
  import org.apache.drill.common.expression.*;
}

@members{
  public static void p(String s){
    System.out.println(s);
  }
}

parse returns [LogicalExpression e]
  :  expression EOF {$e = $expression.e; }
  ;
 
functionCall returns [LogicalExpression e]
  :  Identifier OParen exprList? CParen {$e = ExpressionFunction.create($Identifier.text, $exprList.listE);  }
  ;

ifStatement returns [LogicalExpression e]
	@init {
	  IfExpression.Builder s = IfExpression.newBuilder();
	}
	@after {
	  $e = s.build();
	}  
  :  i1=ifStat {s.addCondition($i1.i); } (elseIfStat { s.addCondition($elseIfStat.i); } )* Else expression { s.setElse($expression.e); }End 
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
	}
	@after{
	  if(exprs.size() == 1){
	    $e = exprs.get(0);
	  }else{
	    $e = new BooleanFunctions.Or(exprs);
	  }
	}
  :  a1=andExpr { exprs.add($a1.e); } (Or^ a2=andExpr { exprs.add($a2.e); })*
  ;

andExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	}
	@after{
	  if(exprs.size() == 1){
	    $e = exprs.get(0);
	  }else{
	    $e = new BooleanFunctions.And(exprs);
	  }
	}
  :  e1=equExpr { exprs.add($e1.e);  } (And^ e2=equExpr { exprs.add($e2.e);  })*
  ;

equExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	  List<String> cmps = new ArrayList();
	}
	@after{
	  $e = BooleanFunctions.Comparison.create(exprs, cmps);
	}
  :  r1=relExpr {exprs.add($r1.e);} ( cmpr= (Equals | NEquals ) r2=relExpr {exprs.add($r2.e); cmps.add($cmpr.text); })*
  ;

relExpr returns [LogicalExpression e]
  :  left=addExpr {$e = $left.e; } (cmpr = (GTEquals | LTEquals | GT | LT) right=addExpr {$e = new BooleanFunctions.Comparison($cmpr.text, $left.e, $right.e); } )? 
  ;

addExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	  List<String> ops = new ArrayList();
	}
	@after{
	  $e = MathFunction.create(exprs, ops);
	}
  :  m1=mulExpr  {exprs.add($m1.e);} ( op=(Plus|Minus) m2=mulExpr {exprs.add($m2.e); ops.add($op.text); })* 
  ;

mulExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	  List<String> ops = new ArrayList();
	}
	@after{
	  $e = MathFunction.create(exprs, ops);
	}
  :  p1=powExpr  {exprs.add($p1.e);} (op=(Asterisk|ForwardSlash|Percent) p2=powExpr {exprs.add($p2.e); ops.add($op.text); } )*
  ;

powExpr returns [LogicalExpression e]
	@init{
	  List<LogicalExpression> exprs = new ArrayList<LogicalExpression>();
	  List<String> ops = new ArrayList();
	}
	@after{
	  $e = MathFunction.create(exprs, ops);
	}
  :  u1=unaryExpr {exprs.add($u1.e);} (Caret u2=unaryExpr {exprs.add($u2.e); ops.add($Caret.text);} )*
  ;
  
unaryExpr returns [LogicalExpression e]
  :  Minus atom {$e = new UnaryFunctions.Negative($atom.e); }
  |  Excl atom {$e= new UnaryFunctions.Not($atom.e); }
  |  atom {$e = $atom.e; }
  ;

atom returns [LogicalExpression e]
  :  Number {$e = new ValueExpressions.NumberExpression($Number.text); }
  |  Bool {$e = new ValueExpressions.BooleanExpression( $Bool.text ); }
  |  lookup {$e = $lookup.e; }
  ;


lookup returns [LogicalExpression e]
  :  functionCall {$e = $functionCall.e ;}
  | Identifier {$e = new ValueExpressions.Identifier($Identifier.text); }
  | String {$e = new ValueExpressions.QuotedString($String.text); }
  | OParen expression CParen  {$e = $expression.e; }
  | SingleQuote Identifier SingleQuote {$e = new ValueExpressions.Identifier($Identifier.text); }
  ;
