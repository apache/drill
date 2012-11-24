lexer grammar ExprLexer;

options {
    language=Java;
}

@header {
  package org.apache.drill.common.expression.parser;
}

If       : 'if';
Else     : 'else';
Return   : 'return';
Then     : 'then';
End      : 'end';
In       : 'in';
Case     : 'case';
When     : 'when';


Or       : '||';
And      : '&&';
Equals   : '==';
NEquals  : '!=';
GTEquals : '>=';
LTEquals : '<=';
Caret      : '^';
Excl     : '!';
GT       : '>';
LT       : '<';
Plus      : '+';
Minus : '-';
Asterisk : '*';
ForwardSlash   : '/';
Percent  : '%';
OBrace   : '{';
CBrace   : '}';
OBracket : '[';
CBracket : ']';
OParen   : '(';
CParen   : ')';
SColon   : ';';
Comma    : ',';
QMark    : '?';
Colon    : ':';
SingleQuote: '\'';

Bool
  :  'true' 
  |  'false'
  ;

Number
  :  Int ('.' Digit*)? ('e' ('+' | '-')? Digit*)?
  ;

//Float
//  :  Int ('.' Digit*)? ('e' ('+' | '-')? Digit*)?
//  ;
//
//Integer
//  :  Digit Digit*
//  ;
  
Identifier
  :  ('a'..'z' | 'A'..'Z' | '_') ('a'..'z' | 'A'..'Z' | '_' | Digit)* ('.' ('a'..'z' | 'A'..'Z' | '_') ('a'..'z' | 'A'..'Z' | '_' | Digit)*)*
  ;

String
@after {
  setText(getText().substring(1, getText().length()-1).replaceAll("\\\\(.)", "$1"));
}
  :  '"'  (~('"' | '\\')  | '\\' ('\\' | '"'))* '"' 
  |  '\'' (~('\'' | '\\') | '\\' ('\\' | '\''))* '\''
  ;

Comment
  :  '//' ~('\r' | '\n')* {skip();}
  |  '/*' .* '*/'         {skip();}
  ;

Space
  :  (' ' | '\t' | '\r' | '\n' | '\u000C') {skip();}
  ;

fragment Int
  :  '1'..'9' Digit*
  |  '0'
  ;
  
fragment Digit 
  :  '0'..'9'
  ;
