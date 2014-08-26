%option noyywrap

%{
#include <math.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include "parseTree.hh"
#include "QProc.hh"
#include "parser.h"
%}

D			[0-9]
IDENTIFIER	[a-zA-Z][a-zA-Z0-9_]*

%%

{D}+	{
				yylval.i_value = atoi(yytext);
				return INTEGER;
			}

{D}+"."{D}*?(E{D}+)?	{
				yylval.d_value = atof(yytext);
				return FLOAT;
			}

{D}+("."{D}*?(E{D}+)?)?[kKmMgGtTpP] {
				char* typeptr = yytext + strlen(yytext) - 1;
				int exp = 0;
				if (*typeptr == 'p' || *typeptr == 'P') exp = 5;
				if (*typeptr == 't' || *typeptr == 'T') exp = 4;
				if (*typeptr == 'g' || *typeptr == 'G') exp = 3;
				if (*typeptr == 'm' || *typeptr == 'M') exp = 2;
				if (*typeptr == 'k' || *typeptr == 'K') exp = 1;
				yylval.d_value = atof(yytext) * pow(1024, exp);
				return FLOAT;
			}

\"?{D}{D}{D}{D}-{D}{D}-{D}{D}\"? {
				struct tm theDate;
				bzero(&theDate, sizeof(struct tm));
				theDate.tm_isdst = -1; //auto-detect DST
				char* ptr = yytext;
				if (yytext[0] == '\"') {
					ptr++;
				}
				strptime(ptr, "%Y-%m-%d", &theDate);
				time_t seconds = mktime(&theDate);
				yylval.i_value = seconds;
				return DATE;
			}

\"?{D}{D}:{D}{D}(:{D}{D})?\"? {
				int blocks[3] = {0,0,0};
				int seconds = 0;
				int block_idx = 0;
				char* s_ptr = yytext;
				if ( *s_ptr == '\"') s_ptr++;
				for (char* ptr = s_ptr; (ptr-yytext)<=yyleng; ptr++) {
					if (*ptr == ':' || *ptr == 0 || *ptr == '\"' || ptr-yytext == yyleng) {
						*ptr = 0;
						blocks[block_idx++] = atoi(s_ptr);
						s_ptr = ptr+1;
						ptr = s_ptr;
					}
				}
				seconds = blocks[0]*3600 + blocks[1]*60 + blocks[2];
				yylval.i_value = seconds;
				return TIME;
			}

\"?{D}{D}{D}{D}-{D}{D}-{D}{D}" "{D}{D}:{D}{D}:{D}{D}\"? {
				struct tm theDate;
				bzero(&theDate, sizeof(struct tm));
				theDate.tm_isdst = -1; //auto-detect DST
				char* ptr = yytext;
				if (yytext[0] == '\"') {
					ptr++;
				}
				strptime(ptr, "%Y-%m-%d %H:%M:%S", &theDate);
				time_t seconds = mktime(&theDate);
				yylval.i_value = seconds;
				return DATETIME;
			}


\"[^"]*?\"		{
				yytext[strlen(yytext)-1] = 0;
				yylval.s_value = strdup(yytext+1);
				return STRING;
			}

{IDENTIFIER}	{
				yylval.s_value = strdup(yytext);
				return NAME;
			}

"+"		return PLUS;
"-"		return MINUS;
"**"	return EXPONENT;
"*"		return TIMES;
"/"		return DIVIDE;
"%"		return MODULUS;
"^"		return EXPONENT;
"=="	return EQUAL;
"!="	return NEQUAL;
"<="	return LESSEQUAL;
">="	return GREATEREQUAL;
"<"		return LESS;
">"		return GREATER;
"="		return ASSIGN;
"&&"	return AND;
"||"	return OR;
"!"		return NOT;
"("		return LPAREN;
")"		return RPAREN;
","		return COMMA;

[ \t\n]+		/* skip whitespace */
.				printf("Unrecognized character: %s\n", yytext);

%%

