/* test accounting data parser */

/* BISON Declarations */

%{
#include "parseTree.hh"
#include "QProc.hh"
#include <iostream>
#include <string>
#include <stack>

void yyerror(const char* error);
extern int yylex (void);
extern QProcConfiguration* configPtr;
extern bool defineSymbolsAllowed;

std::stack<Expression*> parseStack;

%}

%token NAME
%token FLOAT
%token INTEGER
%token DATE
%token DATETIME
%token TIME
%token STRING
%token PLUS
%token MINUS
%token TIMES
%token DIVIDE
%token MODULUS
%token EXPONENT
%token EQUAL
%token NEQUAL
%token LESS
%token GREATER
%token LESSEQUAL
%token GREATEREQUAL
%token AND
%token OR
%token NOT
%token LPAREN
%token RPAREN
%token ASSIGN
%token COMMA

%left COMMA
%left AND OR
%right NOT
%left EQUAL NEQUAL LESS GREATER LESSEQUAL GREATEREQUAL
%left ASSIGN
%left PLUS MINUS
%left TIMES DIVIDE MODULUS
%right EXPONENT
%right UMINUS

%union {
	Expression* exp;
	ExpressionList* expList;
	intType       i_value;
	double    d_value;
	char*     s_value;
}

/* Lets inform Bison about the type of each terminal and non-terminal */
%type <exp>   expression
%type <expList> exprList
%type <i_value> INTEGER
%type <i_value> DATETIME
%type <i_value> DATE
%type <i_value> TIME
%type <d_value> FLOAT
%type <s_value> STRING
%type <s_value> NAME


%%

expression :  MINUS expression %prec UMINUS { $$ = new BinExpression(TYPE_TIMES, new NumericalExpression((intType) -1), $2); parseStack.pop(); parseStack.push($$); }
            | NOT expression                { $$ = new MonExpression(TYPE_NOT, $2); parseStack.pop(); parseStack.push($$); }
            | LPAREN expression RPAREN      { $$ = new MonExpression(TYPE_GROUP, $2); parseStack.pop(); parseStack.push($$); }
            | STRING                        { $$ = new StringExpression($1, true); parseStack.push($$); }
			| NAME							{ $$ = new NameExpression($1, configPtr, defineSymbolsAllowed); parseStack.push($$); }
            | INTEGER                       { $$ = new NumericalExpression($1); parseStack.push($$); }
            | DATETIME                      { $$ = new NumericalExpression($1); parseStack.push($$); }
            | DATE                          { $$ = new NumericalExpression($1); parseStack.push($$); }
            | TIME                          { $$ = new NumericalExpression($1); parseStack.push($$); }
			| FLOAT                         { $$ = new NumericalExpression($1); parseStack.push($$); }
			| expression PLUS expression    { $$ = new BinExpression(TYPE_PLUS, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression MINUS expression   { $$ = new BinExpression(TYPE_MINUS, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression TIMES expression   { $$ = new BinExpression(TYPE_TIMES, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression DIVIDE expression  { $$ = new BinExpression(TYPE_DIVIDE, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression MODULUS expression { $$ = new BinExpression(TYPE_MODULUS, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression EXPONENT expression{ $$ = new BinExpression(TYPE_EXPONENT, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression EQUAL expression   { $$ = new BinExpression(TYPE_EQUAL, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression NEQUAL expression  { $$ = new BinExpression(TYPE_NEQUAL, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression LESS expression    { $$ = new BinExpression(TYPE_LESS, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression GREATER expression { $$ = new BinExpression(TYPE_GREATER, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression LESSEQUAL expression { $$ = new BinExpression(TYPE_LESSEQUAL, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression GREATEREQUAL expression { $$ = new BinExpression(TYPE_GREATEREQUAL, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression AND expression     { $$ = new BinExpression(TYPE_AND, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression OR expression      { $$ = new BinExpression(TYPE_OR, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| expression ASSIGN expression  { $$ = new BinExpression(TYPE_ASSIGN, $1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
			| NAME LPAREN expression RPAREN { $$ = new FxnExpression($1, $3, configPtr); parseStack.pop(); parseStack.push($$); }
			| NAME LPAREN exprList RPAREN   { $$ = new FxnExpression($1, $3, configPtr); parseStack.pop(); parseStack.push($$); }
		    | exprList                      { $$ = $1; }

exprList : expression COMMA expression      { $$ = new ExpressionList($1, $3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }
         | exprList COMMA expression        { $$ = static_cast<ExpressionList*>($1); $1->addExpression($3); parseStack.pop(); parseStack.pop(); parseStack.push($$); }


%%

void yyerror(const char* error) {
	std::cout << error << std::endl;
}
