grammar Plan;

options {
  output = AST;
}

@header {
package org.apache.drill.plan.ast;
import com.google.common.collect.Lists;
}

@lexer::header {
package org.apache.drill.plan.ast;
}

@members {
  public void reportError(RecognitionException e) {
    throw new LogicalPlanParseException("Syntax error in schema: ", e);
  }
}

plan returns [Plan r]: s=statements EOF {$r = $s.r;};

statements returns [Plan r]:
     s1 = statement {$r = Plan.create($s1.r);}
     ( s2 = statement {$r = $r.add($s2.r);} )*
     ;

statement returns [Op r]:
     to=targets GETS o=OP from=args LINE_ENDING {$r = Op.create($o.text, $from.r, $to.r);}
     | LINE_ENDING {$r = null; };

targets returns [List<Arg> r]:
     a = symbol {$r = Lists.newArrayList($a.r);} ( COMMA b = symbol {$r.add($b.r);} )* ;

symbol returns [Arg r]:
     s = SYMBOL { $r = Arg.createSymbol($s.text); };

args returns [List<Arg> r]: a = arg {$r = Lists.newArrayList($a.r);} ( COMMA b = arg {$r.add($b.r);} )* ;

arg returns [Arg r]:
    s = STRING {$r = Arg.createString($s.text);}
    | n = NUMBER {$r = Arg.createNumber($n.text);}
    | b = BOOLEAN {$r = Arg.createBoolean($b.text);}
    | s = SYMBOL {$r = Arg.createSymbol($s.text);}
    ;


STRING: ('"'|'Ò') ( ~('"' | '\\') | '\\' .)* ('"'|'Ó') ;
GETS: ':=' ;
BOOLEAN: 'true'|'false';
SYMBOL: '%' ('0'..'9')+;
OP: ('a'..'z'|'A'..'Z') ('a'..'z'|'A'..'Z'|'-')*
    | '>' | '<' | '>=' | '<=' | '+' | '-' | '*' | '/';
COMMA: ',' ;
NUMBER: ('0'..'9')+ ;
LINE_ENDING: '\r'? '\n';
COMMENT: '#' (~'\n')* {$channel=HIDDEN;} ;
WHITESPACE : ( '\t' | ' ' )+ { $channel = HIDDEN; } ;
