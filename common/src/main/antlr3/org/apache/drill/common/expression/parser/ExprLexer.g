lexer grammar ExprLexer;

options {
    language=Java;
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
}

If       : 'if';
Else     : 'else';
Return   : 'return';
Then     : 'then';
End      : 'end';
In       : 'in';
Case     : 'case';
When     : 'when';

Cast: 'cast';
Nullable: 'nullable';
Repeat: 'repeat';
As: 'as';

INT		 : 'int';
BIGINT	 : 'bigint';
FLOAT4   : 'float4';
FLOAT8   : 'float8';
VARCHAR  : 'varchar';
VARBINARY: 'varbinary';
DATE     : 'date';
TIMESTAMP: 'timestamp';
TIME     : 'time';
TIMESTAMPTZ: 'timestamptz';
INTERVAL : 'interval';
INTERVALYEAR : 'intervalyear';
INTERVALDAY : 'intervalday';

Or       : '||' | 'or' | 'OR' | 'Or';
And      : '&&' | 'and' | 'AND' ;
Equals   : '==' | '=';
NEquals  : '<>' | '!=';
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
  :  ('a'..'z' | 'A'..'Z' | '_' | '$') ('a'..'z' | 'A'..'Z' | '_' | '$' | Digit)* ('.' ('a'..'z' | 'A'..'Z' | '_' | '$' ) ('a'..'z' | 'A'..'Z' | '_' | '$' | Digit)*)*
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
