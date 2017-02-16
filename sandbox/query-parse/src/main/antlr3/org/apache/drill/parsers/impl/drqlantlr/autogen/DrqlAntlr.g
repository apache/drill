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

grammar DrqlAntlr;

options {output=AST; ASTLabelType=CommonTree; backtrack=false; k=1;}

tokens {
N_SELECT_STATEMENT; 
N_SELECT; N_COLUMN; N_COLUMN_NAME; N_ALIAS; N_WITHIN; N_WITHIN_RECORD;
N_FROM; N_TABLE; N_TABLE_NAME;
N_JOIN; N_INNER;N_LEFTOUTER; N_JOIN_ON; N_JOIN_ON_LIST;
N_WHERE; 
N_GROUPBY; 
N_HAVING; 
N_ORDERBY; N_ASC; N_DESC;
N_LIMIT; 
N_EXPRESSION; N_ID; N_NAME;N_IN; N_CALL; N_OP;N_LOGICAL_OR;N_LOGICAL_AND;
N_BITWISE_OR;N_BITWISE_XOR;N_BITWISE_AND;N_EQUAL;N_NOT_EQUAL;N_LESS_THAN;
N_LESS_THAN_OR_EQUAL;N_GREATER_THAN;N_LOGICAL_NOT;N_BITWISE_NOT;
N_CONTAINS;N_REMAINDER;N_GREATER_THAN_OR_EQUAL;N_BITWISE_RIGHT_SHIFT;
N_DIVIDE;N_MULTIPLY;N_SUBSTRUCT;N_ADD;N_BITWISE_RIGHT_SHIFT;
N_BITWISE_LEFT_SHIFT;N_IN_PARAMS;N_CALL_PARAMS;N_INT; N_FLOAT; N_STRING;
}

@header {package org.apache.drill.parsers.impl.drqlantlr.autogen;}

@lexer::header {package org.apache.drill.parsers.impl.drqlantlr.autogen;}

///Starting production 
request: selectStatement (SEMICOLON!)? EOF!;

//Select statement
selectStatement: selectClause fromClause joinClause? whereClause? 
    havingClause? groupbyClause? orderbyClause? limitClause?-> 
        ^(N_SELECT_STATEMENT fromClause selectClause joinClause? whereClause? 
        groupbyClause? havingClause? orderbyClause? limitClause?);

//Select clause
selectClause: SELECT columnExpr (COMMA columnExpr)* ->
    ^(N_SELECT columnExpr+);
    
//Expression
columnExpr: expr withinClause? (AS ID)? -> 
    ^(N_COLUMN expr ^(N_ALIAS ID)? withinClause? );
withinClause: (WITHIN (RECORD -> ^(N_WITHIN_RECORD) | columnPath ->
	^(N_WITHIN columnPath)));
	
//From clause
fromClause: FROM subSelectStatement (COMMA subSelectStatement)* ->
    ^(N_FROM subSelectStatement+);
subSelectStatement : (tableName | (LPAREN! selectStatement RPAREN!));

//Join clause (TODO add productions)
joinClause: joinClauseDesc JOIN joinClauseFrom ON joinConditionList ->
    ^(N_JOIN joinClauseDesc joinClauseFrom joinConditionList);
joinClauseDesc: (INNER?) -> ^(N_INNER) | (LEFT OUTER) -> ^(N_LEFTOUTER);
joinClauseFrom: optionallyAliasedTable -> ^(N_TABLE optionallyAliasedTable) | 
    aliasedSubSelectStatement -> ^(N_TABLE aliasedSubSelectStatement);
optionallyAliasedTable: tableName (AS ID)? -> ^(N_TABLE tableName ID?);
aliasedSubSelectStatement:	LPAREN selectStatement RPAREN AS tableName ->
    ^(N_TABLE selectStatement tableName);
joinConditionList:	joinCondition (LOGICAL_AND joinCondition)* ->
    ^(N_JOIN_ON_LIST joinCondition+);
joinCondition: a=columnPath EQUAL b=columnPath -> ^(N_JOIN_ON $a $b);

//Where Clause 
whereClause: WHERE expr -> ^(N_WHERE expr);

//Groupby Clause
groupbyClause: GROUPBY columnName (COMMA columnName)* -> 
    ^(N_GROUPBY columnName+);

//Having Clause
havingClause: HAVING expr -> ^(N_HAVING expr);

//Orderby Clause
orderbyClause: ( ORDERBY orderbyColumnName (COMMA orderbyColumnName)*)	->
    ^(N_ORDERBY orderbyColumnName+);
orderbyColumnName:	columnName (ASC -> ^(N_ASC columnName) | DESC ->
    ^(N_DESC columnName) | /* default sort order */	->	^(N_ASC columnName));
						
//Limit clause
limitClause: ( LIMIT INT ) -> ^(N_LIMIT INT);

///Column names
columnPath: columnPath2 -> ^(N_ID columnPath2);
columnPath2: columnName (DOT! columnName)*;
columnName: ID -> ^(N_NAME ID) | STAR -> ^(N_NAME STAR);

//Table names
tablePath: tablePath2 -> ^(N_TABLE tablePath2);
tablePath2:	tableName (DOT! tableName)*;						
tableName: ID -> ^(N_TABLE_NAME ID);

//Expressions
expr: expr2 -> ^(N_EXPRESSION expr2);
expr2:(a=b10e->$a) (o=b11o b=b10e -> ^($o $expr2 $b))*;
b10e: (a=b9e->$a) (o=b10o b=b9e -> ^($o $b10e $b))*;
b9e:  (a=b8e->$a) (o=b9o b=b8e  -> ^($o $b9e $b))*;
b8e:  (a=b7e->$a) (o=b8o b=b7e 	-> ^($o $b8e $b))*;
b7e:  (a=b6e->$a) (o=b7o b=b6e 	-> ^($o $b7e $b))*;
b6e:  (a=b5e->$a) (o=b6o b=b5e 	-> ^($o $b6e $b))*;
b5e:  (a=b4e->$a) (o=b5o b=b4e 	-> ^($o $b5e $b))*;
b4e:  (a=b3e->$a) (o=b4o b=b3e 	-> ^($o $b4e $b))*;
b3e:  (a=b2e->$a) (o=b3o b=b2e  -> ^($o $b3e $b))*;
b2e:  (a=b1e->$a) (o=b2o b=b1e 	-> ^($o $b2e $b))*;
b1e:  (a=uPrefixExpr->$a)   (o=b1o b=uPrefixExpr -> ^($o $b1e $b))*;
uPrefixExpr: (o=uPrefixOp->^($o $uPrefixExpr))* (a=uPostfixExpr -> $a); 
uPostfixExpr: (a=atomExpr->$a) (o=uPostfixOp -> ^($o $uPostfixExpr))*;
atomExpr: INT -> ^(N_INT INT)
    | FLOAT -> ^(N_FLOAT FLOAT)
    | STRING -> ^(N_STRING STRING)  
    | (LPAREN expr RPAREN) -> expr
    | columnPath;

//Ops
b11o: LOGICAL_OR ->	N_LOGICAL_OR;	
b10o: LOGICAL_AND->	N_LOGICAL_AND;
b9o:  BITWISE_OR ->	N_BITWISE_OR;
b8o:  BITWISE_XOR->	N_BITWISE_XOR;
b7o:  BITWISE_AND->	N_BITWISE_AND;
b6o:  EQUAL -> N_EQUAL | NOT_EQUAL -> N_NOT_EQUAL;
b5o:  LESS_THAN -> N_LESS_THAN | LESS_THAN_OR_EQUAL -> N_LESS_THAN_OR_EQUAL | 
    GREATER_THAN -> N_GREATER_THAN | 
    GREATER_THAN_OR_EQUAL -> N_GREATER_THAN_OR_EQUAL;
b4o: BITWISE_LEFT_SHIFT -> N_BITWISE_LEFT_SHIFT | 
    BITWISE_RIGHT_SHIFT -> N_BITWISE_RIGHT_SHIFT;
b3o: ADD -> N_ADD | SUBSTRUCT -> N_SUBSTRUCT;
b2o: multiplyOp -> N_MULTIPLY | 
    divideOp ->	N_DIVIDE | REMAINDER -> N_REMAINDER;
b1o: CONTAINS -> N_CONTAINS; 
uPrefixOp: BITWISE_NOT -> N_BITWISE_NOT | LOGICAL_NOT -> N_LOGICAL_NOT;
uPostfixOp:	uPostfixOpIn | uPostfixOpCall;
uPostfixOpIn: IN LPAREN (expr (COMMA expr)*)? RPAREN ->	^(N_IN_PARAMS expr*);
uPostfixOpCall:	LPAREN (expr (COMMA expr)*)? RPAREN -> ^(N_CALL_PARAMS expr*);

//Name clash grapheme ops
divideOp: SLASH | DIV;
multiplyOp: STAR;


//Keywords
SELECT		:	S E L E C T ;
WITHIN		:	W I T H I N ;
RECORD		:	R E C O R D ;
AS			:A S ;
FROM		:	F R O M ;
INNER		:	I N N E R;
LEFT		:	L E F T;
OUTER		:	O U T E R;
JOIN		:	J O I N;
ON			:	O N;
WHERE		:	W H E R E ;
GROUPBY		:	G R O U P WS B Y;	
HAVING		:	H A V I N G;
ORDERBY		:	O R D E R WS B Y;
DESC		:	D E S C;
ASC			:	A S C;
LIMIT		:	L I M I T;
LOGICAL_OR 	:	O R;
LOGICAL_AND	:	A N D;
LOGICAL_NOT	:	N O T;
CONTAINS 	:	C O N T A I N S ;
IN		:	I N;

//Graphemes
BITWISE_AND : 	'&';
BITWISE_NOT	: 	'~';
BITWISE_OR	: 	'|';
BITWISE_XOR : 	'^';
EQUAL 		: 	'=' | '==';
NOT_EQUAL 	: 	'<>' | '!=';
LESS_THAN_OR_EQUAL:	'<=';
LESS_THAN 	: 	'<';
GREATER_THAN_OR_EQUAL: 	'>=';
GREATER_THAN: 	'>';
SLASH 		: 	'/';
DIV			:	' D I V';
STAR		: 	'*';
ADD 		: 	'+';
SUBSTRUCT 	:	'-';
REMAINDER	: 	'%';
BITWISE_LEFT_SHIFT: 	'<<';
BITWISE_RIGHT_SHIFT: 	'>>';
DOT		: 	'.'; 
COLON		: 	':';
COMMA		: 	',';
SEMICOLON	: 	';';
LPAREN 		: 	'(';
RPAREN 		: 	')';
LSQUARE 	: 	'[' ;
RSQUARE 	: 	']' ;
LCURLY 		: 	'{';
RCURLY 		: 	'}';

//Lexemes
ID: F_ID1 | F_ID2;
fragment F_ID1: ('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')* ;
fragment F_ID2: '[' ('a'..'z'|'A'..'Z'|'0'..'9'|'_'|' '|'\\'|'/'|'-'|'+'|'*'|'.'|':'|'$')* ']';

INT :	'0'..'9'+
    ;

FLOAT
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
    |   '.' ('0'..'9')+ EXPONENT?
    |   ('0'..'9')+ EXPONENT
    ;

COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;};

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {$channel=HIDDEN;}
    ;

STRING
    :  '\'' ( ESC_SEQ | ~('\\'|'\'') )* '\''
    ;

fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

//Case insensetive letters, pretty ugly (or beautiful) but I haven't found a better way, sorry
fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');    