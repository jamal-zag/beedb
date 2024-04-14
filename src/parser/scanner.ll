%{
    #include <parser/driver.h>
    #include <parser/scanner.hpp>
    #include <parser/parser.hpp>
    #include <parser/location.hh>
    #include <table/date.h>

    static beedb::parser::location loc;

    #define YY_USER_ACTION loc.step(); loc.columns(yyleng);

    #undef  YY_DECL
    #define YY_DECL beedb::parser::Parser::symbol_type beedb::parser::Scanner::lex(beedb::parser::Driver &/*driver*/)

    #define yyterminate() return beedb::parser::Parser::make_END(loc);
%}


%option c++
%option yyclass="beedb::parser::Scanner"
%option noyywrap


%%
\(						            { return Parser::make_LEFT_PARENTHESIS_TK(loc); }
\)						            { return Parser::make_RIGHT_PARENTHESIS_TK(loc); }
\,                                  { return Parser::make_COMMA_TK(loc); }
\.                                  { return Parser::make_DOT_TK(loc); }
CREATE                              { return Parser::make_CREATE_TK(loc); }
TABLE                               { return Parser::make_TABLE_TK(loc); }
INDEX                               { return Parser::make_INDEX_TK(loc); }
IF                                  { return Parser::make_IF_TK(loc); }
NOT                                 { return Parser::make_NOT_TK(loc); }
EXISTS                              { return Parser::make_EXISTS_TK(loc); }
ON                                  { return Parser::make_ON_TK(loc); }
UNIQUE                              { return Parser::make_UNIQUE_TK(loc); }
INSERT                              { return Parser::make_INSERT_TK(loc); }
INTO                                { return Parser::make_INTO_TK(loc); }
VALUES                              { return Parser::make_VALUES_TK(loc); }
UPDATE                              { return Parser::make_UPDATE_TK(loc); }
SET                                 { return Parser::make_SET_TK(loc); }
DELETE                              { return Parser::make_DELETE_TK(loc); }
NULL                                { return Parser::make_NULL_TK(loc); }
INT|INTEGER                         { return Parser::make_INT_TK(loc); }
LONG                                { return Parser::make_LONG_TK(loc); }
DATE                                { return Parser::make_DATE_TK(loc); }
DECIMAL                             { return Parser::make_DECIMAL_TK(loc); }
CHAR                                { return Parser::make_CHAR_TK(loc); }
SELECT                              { return Parser::make_SELECT_TK(loc); }
FROM                                { return Parser::make_FROM_TK(loc); }
AS                                  { return Parser::make_AS_TK(loc); }
JOIN                                { return Parser::make_JOIN_TK(loc); }
WHERE                               { return Parser::make_WHERE_TK(loc); }
AND                                 { return Parser::make_AND_TK(loc); }
OR                                  { return Parser::make_OR_TK(loc); }
"="                                 { return Parser::make_EQUALS_TK(loc); }
"!="|"<>"                           { return Parser::make_NOT_EQUALS_TK(loc); }
"<"                                 { return Parser::make_LESSER_TK(loc); }
"<="                                { return Parser::make_LESSER_EQUALS_TK(loc); }
">"                                 { return Parser::make_GREATER_TK(loc); }
">="                                { return Parser::make_GREATER_EQUALS_TK(loc); }
COUNT                               { return Parser::make_COUNT_TK(loc); }
SUM                                 { return Parser::make_SUM_TK(loc); }
AVG                                 { return Parser::make_AVG_TK(loc); }
MIN                                 { return Parser::make_MIN_TK(loc); }
MAX                                 { return Parser::make_MAX_TK(loc); }
"GROUP BY"                          { return Parser::make_GROUP_BY_TK(loc); }
"ORDER BY"                          { return Parser::make_ORDER_BY_TK(loc); }
ASC                                 { return Parser::make_ASC_TK(loc); }
DESC                                { return Parser::make_DESC_TK(loc); }
LIMIT                               { return Parser::make_LIMIT_TK(loc); }
OFFSET                              { return Parser::make_OFFSET_TK(loc); }
"+"                                 { return Parser::make_PLUS_TK(loc); }
"-"                                 { return Parser::make_MINUS_TK(loc); }
"/"                                 { return Parser::make_DIV_TK(loc); }
"*"                                 { return Parser::make_MUL_TK(loc); }
BEGIN                               { return Parser::make_BEGIN_TK(loc); }
COMMIT                              { return Parser::make_COMMIT_TK(loc); }
ABORT                               { return Parser::make_ABORT_TK(loc); }
END                                 { return Parser::make_END_TK(loc); }
\'[0-9]{4}\-[0-9]{2}\-[0-9]{2}\'    { return Parser::make_DATE(table::Date::from_string(std::string{yytext}.substr(1, std::strlen(yytext)-2)), loc); }
\'[0-9]+\'                          { return Parser::make_STRING_INTEGER(std::stoll(std::string{yytext}.substr(1, std::strlen(yytext)-2)), loc); }
\'[^\']+\'                          { return Parser::make_STRING(std::string{yytext}.substr(1, std::strlen(yytext)-2), loc); }
[a-zA-Z_][a-zA-Z_0-9]*	            { return Parser::make_REFERENCE(yytext, loc); }
[0-9]+				                { return Parser::make_UNSIGNED_INTEGER(std::stoll(yytext), loc); }
-?[0-9]+				            { return Parser::make_INTEGER(std::stoll(yytext), loc); }
-?[0-9]*\.[0-9]*                    { return Parser::make_DECIMAL(std::stod(std::string{yytext}), loc); }
\n                                  { return Parser::make_END(loc); }
<<EOF>>                             { return Parser::make_END(loc); }
.                                   { }

%%

