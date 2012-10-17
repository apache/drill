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

@lexer::members {
  Stack<String> paraphrase = new Stack<String>();
}

@members {
  Stack<String> paraphrase = new Stack<String>();
  public void reportError(RecognitionException e) {
      throw new LogicalPlanParseException(
           String.format("Syntax error in schema line \%d:\%d ", e.line, e.charPositionInLine),
           e);
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
    | z = HEX_LONG {$r = Arg.createLong($z.text);}
    | n = NUMBER {$r = Arg.createNumber($n.text);}
    | b = BOOLEAN {$r = Arg.createBoolean($b.text);}
    | s = SYMBOL {$r = Arg.createSymbol($s.text);}
    ;


STRING
@init { paraphrase.push("a string"); }
@after { paraphrase.pop(); }
  : ('"'|'\u201c') ( ~('"' | '\\') | '\\' .)* ('"'|'\u201d') ;

HEX_LONG
@init { paraphrase.push("a binary string"); }
@after { paraphrase.pop(); }
  : '0x' ( '0'..'9' | 'A'..'F' | 'a'..'f')+ ;

GETS
@init { paraphrase.push(":="); }
@after { paraphrase.pop(); }
  : ':=' ;

BOOLEAN
@init { paraphrase.push("a boolean value"); }
@after { paraphrase.pop(); }
  : 'true'|'false';

SYMBOL
@init { paraphrase.push("a percent-symbol"); }
@after { paraphrase.pop(); }
  : '%' ('0'..'9')+;

OP
@init { paraphrase.push("an operator"); }
@after { paraphrase.pop(); }
  : ('a'..'z'|'A'..'Z') ('a'..'z'|'A'..'Z'|'-')*
    | '>' | '<' | '>=' | '<=' | '+' | '-' | '*' | '/';

COMMA
@init { paraphrase.push("a comma"); }
@after { paraphrase.pop(); }
  : ',' ;

NUMBER
@init { paraphrase.push("a number"); }
@after { paraphrase.pop(); }
  : ('0'..'9')+ ;

LINE_ENDING
@init { paraphrase.push("an end of line"); }
@after { paraphrase.pop(); }
  : '\r'? '\n';

COMMENT
@init { paraphrase.push("a comment"); }
@after { paraphrase.pop(); }
  : '#' (~'\n')* {$channel=HIDDEN;} ;

WHITESPACE: ( '\t' | ' ' )+ { $channel = HIDDEN; } ;
