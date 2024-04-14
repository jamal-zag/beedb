%skeleton "lalr1.cc"
%require  "3.0"

%defines
%define api.location.file "../../include/parser/location.hh"
%define api.namespace {beedb::parser}
%define api.parser.class {Parser}
%define api.value.type variant
%define api.token.constructor
%define parse.assert true
%define parse.error verbose
%locations

%param {beedb::parser::Driver &driver}
%parse-param {beedb::parser::Scanner &scanner}

%code requires {
    #include <parser/node.h>
    #include <utility>
    #include <vector>
    #include <tuple>
    namespace beedb::parser {
        class Scanner;
        class Driver;
    }

    #define YY_NULLPTR nullptr
}


%{
    #include <cassert>
    #include <iostream>
    #include <memory>
    #include <vector>
    #include <string>
    #include <cstdint>

    #include <parser/driver.h>
    #include <parser/scanner.hpp>

    #include <parser/parser.hpp>
    #include <parser/location.hh>
    #include <exception/parser_exception.h>

    #undef yylex
    #define yylex scanner.lex
%}

%token <std::string> STRING REFERENCE
%token <std::int64_t> INTEGER STRING_INTEGER
%token <std::uint64_t> UNSIGNED_INTEGER
%token <double> DECIMAL
%token <table::Date> DATE
%token NULL_TK INT_TK LONG_TK DATE_TK DECIMAL_TK CHAR_TK
%token LEFT_PARENTHESIS_TK RIGHT_PARENTHESIS_TK
%token COMMA_TK DOT_TK END
%token CREATE_TK
%token TABLE_TK INDEX_TK
%token IF_TK NOT_TK EXISTS_TK
%token ON_TK
%token UNIQUE_TK
%token INSERT_TK INTO_TK VALUES_TK
%token UPDATE_TK SET_TK
%token DELETE_TK
%token SELECT_TK FROM_TK AS_TK JOIN_TK
%token WHERE_TK
%token AND_TK OR_TK
%token EQUALS_TK NOT_EQUALS_TK LESSER_TK LESSER_EQUALS_TK GREATER_TK GREATER_EQUALS_TK
%token COUNT_TK SUM_TK AVG_TK MIN_TK MAX_TK GROUP_BY_TK
%token PLUS_TK MINUS_TK DIV_TK MUL_TK CAST_TK
%token ORDER_BY_TK ASC_TK DESC_TK
%token LIMIT_TK OFFSET_TK
%token BEGIN_TK ABORT_TK COMMIT_TK
%token END_TK

%type <std::unique_ptr<NodeInterface>> query
%type <std::unique_ptr<NodeInterface>> statement
%type <std::unique_ptr<CreateTableStatement>> create_table_statement
%type <std::unique_ptr<CreateIndexStatement>> create_index_statement
%type <std::unique_ptr<InsertStatement>> insert_statement
%type <std::unique_ptr<UpdateStatement>> update_statement
%type <std::unique_ptr<DeleteStatement>> delete_statement
%type <std::unique_ptr<TransactionStatement>> transaction_statement
%type <std::unique_ptr<SelectQuery>> select_query
%type <std::pair<table::Column, expression::Term>> column_description
%type <table::Schema> schema_description
%type <table::Type> type_description
%type <bool> optional_if_not_exists
%type <bool> optional_nullable
%type <bool> optional_unique
%type <std::vector<std::string>> column_names
%type <std::vector<std::string>> optional_column_names
%type <std::vector<std::vector<table::Value>>> values_list
%type <std::vector<table::Value>> values
%type <std::vector<table::Value>> values_with_parenthesis
%type <table::Value> value
%type <std::vector<std::pair<expression::Attribute, std::unique_ptr<expression::Operation>>>> update_list
%type <std::pair<expression::Attribute, std::unique_ptr<expression::Operation>>> update_item
%type <expression::Attribute> update_reference
%type <std::vector<TableDescr>> table_reference_list
%type <TableDescr> table_reference
%type <std::optional<std::vector<JoinDescr>>> optional_join_reference_list
%type <std::vector<JoinDescr>> join_reference_list
%type <JoinDescr> join_reference
%type <std::unique_ptr<expression::Operation>> join_predicate
%type <std::unique_ptr<expression::Operation>> join_operand
%type <std::vector<std::unique_ptr<expression::Operation>>> attribute_list
%type <std::unique_ptr<expression::Operation>> aliased_attribute
%type <std::unique_ptr<expression::Operation>> attribute
%type <std::unique_ptr<expression::Operation>> operand
%type <std::unique_ptr<expression::Operation>> operation
%type <std::unique_ptr<expression::Operation>> optional_where
%type <std::optional<std::vector<expression::Term>>> optional_group_by
%type <std::vector<expression::Term>> group_by_list
%type <expression::Term> group_by_item
%type <std::optional<OrderByExpression>> optional_order_by
%type <OrderByExpression> order_by_list
%type <OrderByItem> order_by_item
%type <std::unique_ptr<expression::NullaryOperation>> order_by_reference
%type <std::optional<LimitExpression>> optional_limit

%left DOT_TK AND_TK OR_TK PLUS_TK MINUS_TK MUL_TK DIV_TK
%nonassoc EQUALS_TK LESSER_TK LESSER_EQUALS_TK GREATER_TK GREATER_EQUALS_TK NOT_EQUALS_TK

%start query
%%

query:
    statement END { driver.ast(std::move($1)); YYACCEPT; }
    | select_query END { driver.ast(std::move($1)); YYACCEPT; }

statement:
    create_table_statement { $$ = std::move($1); }
    | create_index_statement { $$ = std::move($1); }
    | insert_statement { $$ = std::move($1); }
    | update_statement { $$ = std::move($1); }
    | delete_statement { $$ = std::move($1); }
    | transaction_statement { $$ = std::move($1); }

/******************************
 * SELECT
 ******************************/
select_query:
    SELECT_TK attribute_list FROM_TK table_reference_list optional_join_reference_list optional_where optional_group_by optional_order_by optional_limit
    {
        $$ = std::make_unique<SelectQuery>(std::move($2), std::move($4), std::move($5), std::move($6), std::move($7), std::move($8), std::move($9));
    }

table_reference_list:
	table_reference { $$ = std::vector<TableDescr>{}; $$.emplace_back(std::move($1)); }
	| table_reference_list COMMA_TK table_reference { $$ = std::move($1); $$.emplace_back(std::move($3)); }

table_reference:
    REFERENCE { $$ = std::make_pair(std::move($1), Alias{std::nullopt}); }
    | REFERENCE REFERENCE { $$ = std::make_pair(std::move($1), std::make_optional(std::move($2))); }
    | REFERENCE AS_TK REFERENCE { $$ = std::make_pair(std::move($1), std::make_optional(std::move($3))); }

join_reference_list:
    join_reference { $$ = std::vector<JoinDescr>{}; $$.emplace_back(std::move($1)); }
    | join_reference_list join_reference { $$ = std::move($1); $$.emplace_back(std::move($2)); }

join_reference:
    JOIN_TK table_reference ON_TK join_predicate { $$ = std::make_pair(std::move($2), std::move($4)); }

join_predicate:
    join_operand EQUALS_TK join_operand { $$ = expression::BinaryOperation::make_equals(std::move($1), std::move($3)); }

join_operand:
    REFERENCE { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1))); }
    | REFERENCE DOT_TK REFERENCE { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1), std::move($3))); }

optional_join_reference_list:
    join_reference_list { $$ = std::make_optional(std::move($1)); }
    | { $$ = std::nullopt; }

attribute_list:
	aliased_attribute { $$ = std::vector<std::unique_ptr<expression::Operation>>{}; $$.emplace_back(std::move($1)); }
	| attribute_list COMMA_TK aliased_attribute { $$ = std::move($1); $$.emplace_back(std::move($3)); }

aliased_attribute:
    attribute { $$ = std::move($1); }
    | attribute AS_TK REFERENCE { $$ = std::move($1); $$->alias(std::move($3)); }

attribute:
    REFERENCE { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1))); }
    | REFERENCE DOT_TK REFERENCE { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1), std::move($3), true)); }
    | REFERENCE DOT_TK MUL_TK { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1), "*", true)); }
    | MUL_TK { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute("*")); }
    | LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = std::move($2); }
    | attribute PLUS_TK attribute { $$ = expression::BinaryOperation::make_add(std::move($1), std::move($3)); }
    | attribute MINUS_TK attribute { $$ = expression::BinaryOperation::make_sub(std::move($1), std::move($3)); }
    | attribute MUL_TK attribute { $$ = expression::BinaryOperation::make_mul(std::move($1), std::move($3)); }
    | attribute DIV_TK attribute { $$ = expression::BinaryOperation::make_div(std::move($1), std::move($3)); }
    | SUM_TK LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = expression::UnaryOperation::make_sum(std::move($3)); }
    | COUNT_TK LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = expression::UnaryOperation::make_count(std::move($3)); }
    | AVG_TK LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = expression::UnaryOperation::make_avg(std::move($3)); }
    | MIN_TK LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = expression::UnaryOperation::make_min(std::move($3)); }
    | MAX_TK LEFT_PARENTHESIS_TK attribute RIGHT_PARENTHESIS_TK { $$ = expression::UnaryOperation::make_max(std::move($3)); }

operand:
    REFERENCE { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1))); }
    | REFERENCE DOT_TK REFERENCE { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1), std::move($3))); }
    | value { $$ = std::make_unique<expression::NullaryOperation>(expression::Term{std::move($1)}); }
    | LEFT_PARENTHESIS_TK operand RIGHT_PARENTHESIS_TK { $$ = std::move($2); }
    | operand PLUS_TK operand { $$ = expression::BinaryOperation::make_add(std::move($1), std::move($3)); }
    | operand MINUS_TK operand { $$ = expression::BinaryOperation::make_sub(std::move($1), std::move($3)); }
    | operand MUL_TK operand { $$ = expression::BinaryOperation::make_mul(std::move($1), std::move($3)); }
    | operand DIV_TK operand { $$ = expression::BinaryOperation::make_div(std::move($1), std::move($3)); }

operation:
    operand EQUALS_TK operand { $$ = expression::BinaryOperation::make_equals(std::move($1), std::move($3)); }
    | operand LESSER_EQUALS_TK operand { $$ = expression::BinaryOperation::make_lesser_equals(std::move($1), std::move($3)); }
    | operand LESSER_TK operand { $$ = expression::BinaryOperation::make_lesser(std::move($1), std::move($3)); }
    | operand GREATER_EQUALS_TK operand { $$ = expression::BinaryOperation::make_greater_equals(std::move($1), std::move($3)); }
    | operand GREATER_TK operand { $$ = expression::BinaryOperation::make_greater(std::move($1), std::move($3)); }
    | operand NOT_EQUALS_TK operand { $$ = expression::BinaryOperation::make_not_equals(std::move($1), std::move($3)); }
    | LEFT_PARENTHESIS_TK operation RIGHT_PARENTHESIS_TK { $$ = std::move($2); }
    | operation AND_TK operation { $$ = expression::BinaryOperation::make_and(std::move($1), std::move($3)); }
    | operation OR_TK operation { $$ = expression::BinaryOperation::make_or(std::move($1), std::move($3)); }

optional_where:
    WHERE_TK operation { $$ = std::move($2); }
    | { $$ = nullptr; }

optional_group_by:
    GROUP_BY_TK group_by_list { $$ = std::make_optional(std::move($2)); }
    | { $$ = std::nullopt; }

group_by_list:
	group_by_item { $$ = std::vector<expression::Term>{}; $$.emplace_back(std::move($1)); }
	| group_by_list COMMA_TK group_by_item { $$ = std::move($1); $$.emplace_back(std::move($3)); }

group_by_item:
    REFERENCE { $$ = expression::Term::make_attribute(std::move($1)); }
    | REFERENCE DOT_TK REFERENCE { $$ = expression::Term::make_attribute(std::move($1), std::move($3)); }

optional_order_by:
    ORDER_BY_TK order_by_list { $$ = std::make_optional(std::move($2)); }
    | { $$ = std::nullopt; }

order_by_list:
	order_by_item { $$ = OrderByExpression{}; $$.emplace_back(std::move($1)); }
	| order_by_list COMMA_TK order_by_item { $$ = std::move($1); $$.emplace_back(std::move($3)); }

order_by_item:
    order_by_reference ASC_TK { $$ = std::make_pair(std::move($1), true); }
    | order_by_reference DESC_TK { $$ = std::make_pair(std::move($1), false); }
    | order_by_reference { $$ = std::make_pair(std::move($1), true); }

order_by_reference:
    REFERENCE { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1))); }
    | REFERENCE DOT_TK REFERENCE { $$ = std::make_unique<expression::NullaryOperation>(expression::Term::make_attribute(std::move($1), std::move($3))); }


optional_limit:
    LIMIT_TK UNSIGNED_INTEGER { $$ = std::make_optional(LimitExpression{$2, 0U}); }
    | LIMIT_TK UNSIGNED_INTEGER OFFSET_TK UNSIGNED_INTEGER { $$ = std::make_optional(LimitExpression{$2, $4}); }
    | { $$ = std::nullopt; }

/******************************
 * STATEMENTS (create, insert, ...)
 ******************************/

/** CREATE **/
create_table_statement:
    CREATE_TK TABLE_TK optional_if_not_exists REFERENCE LEFT_PARENTHESIS_TK schema_description RIGHT_PARENTHESIS_TK {
        $$ = std::make_unique<CreateTableStatement>(std::move($4), $3, std::move($6));
    }

column_description:
	REFERENCE type_description optional_nullable { $$ = std::make_pair(table::Column{$2, $3}, expression::Term::make_attribute(std::move($1))); }

schema_description:
	column_description { $$ = table::Schema{}; $$.add(std::move(std::get<0>($1)), std::move(std::get<1>($1))); }
	| schema_description COMMA_TK column_description { $$ = std::move($1); $$.add(std::move(std::get<0>($3)), std::move(std::get<1>($3))); }

type_description:
    INT_TK { $$ = table::Type::make_int(); }
    | LONG_TK { $$ = table::Type::make_long(); }
    | DATE_TK { $$ = table::Type::make_date(); }
    | DECIMAL_TK { $$ = table::Type::make_decimal(); }
    | CHAR_TK LEFT_PARENTHESIS_TK UNSIGNED_INTEGER RIGHT_PARENTHESIS_TK { $$ = table::Type::make_char($3); }

optional_if_not_exists:
    IF_TK NOT_TK EXISTS_TK { $$ = true; }
    | { $$ = false; }

optional_nullable:
    NULL_TK { $$ = true; }
    | NOT_TK NULL_TK { $$ = false; }
    | { $$ = false; }

create_index_statement:
    CREATE_TK optional_unique INDEX_TK REFERENCE ON_TK REFERENCE LEFT_PARENTHESIS_TK REFERENCE RIGHT_PARENTHESIS_TK {
        $$ = std::make_unique<CreateIndexStatement>(std::move($4), std::move($6), std::move($8), $2);
    }

optional_unique:
    UNIQUE_TK { $$ = true; }
    | { $$ = false; }

/** INSERT **/
insert_statement:
    INSERT_TK INTO_TK REFERENCE optional_column_names VALUES_TK values_list {
        $$ = std::make_unique<InsertStatement>(std::move($3), std::move($4), std::move($6));
    }

column_names:
	REFERENCE { $$ = std::vector<std::string>{}; $$.emplace_back(std::move($1)); }
	| column_names COMMA_TK REFERENCE { $$ = std::move($1); $$.emplace_back(std::move($3)); }

optional_column_names:
    LEFT_PARENTHESIS_TK column_names RIGHT_PARENTHESIS_TK { $$ = std::move($2); }
    | { $$ = std::vector<std::string>{}; }

values_list:
	values_with_parenthesis { $$ = std::vector<std::vector<table::Value>>{}; $$.emplace_back(std::move($1)); }
	| values_list COMMA_TK values_with_parenthesis { $$ = std::move($1); $$.emplace_back(std::move($3)); }

values_with_parenthesis:
    LEFT_PARENTHESIS_TK values RIGHT_PARENTHESIS_TK { $$ = std::move($2); }

values:
    value { $$ = std::vector<table::Value>{}; $$.emplace_back(std::move($1)); }
    | values COMMA_TK value { $$ = std::move($1); $$.emplace_back(std::move($3)); }

value:
    STRING { $$ = table::Value{table::Type::make_char($1.length()), table::Value::value_type{std::move($1)}}; }
    | INTEGER { $$ = table::Value{table::Type::make_long(), table::Value::value_type{std::int64_t($1)}}; }
    | UNSIGNED_INTEGER { $$ = table::Value{table::Type::make_long(), table::Value::value_type{std::int64_t($1)}}; }
    | STRING_INTEGER { $$ = table::Value{table::Type::make_long(), table::Value::value_type{std::int64_t($1)}}; }
    | DATE { $$ = table::Value{table::Type::make_date(), table::Value::value_type{$1}}; }
    | DATE_TK DATE { $$ = table::Value{table::Type::make_date(), table::Value::value_type{$2}}; }
    | DECIMAL { $$ = table::Value{table::Type::make_decimal(), table::Value::value_type{$1}}; }

/** UPDATE **/
update_statement:
    UPDATE_TK REFERENCE SET_TK update_list optional_where {
        $$ = std::make_unique<UpdateStatement>(std::move($2), std::move($4), std::move($5));
    }

update_list:
    update_item { $$ = std::vector<std::pair<expression::Attribute, std::unique_ptr<expression::Operation>>>{}; $$.emplace_back(std::move($1)); }
	| update_list COMMA_TK update_item { $$ = std::move($1); $$.emplace_back(std::move($3)); }

update_item:
    update_reference EQUALS_TK operand { $$ = std::make_pair(std::move($1), std::move($3)); }

update_reference:
    REFERENCE { $$ = expression::Attribute{std::move($1)}; }

/** DELETE **/
delete_statement:
    DELETE_TK FROM_TK REFERENCE optional_where {
        $$ = std::make_unique<DeleteStatement>(std::move($3), std::move($4));
    }

/** TRANSACTION **/
transaction_statement:
    BEGIN_TK {
        $$ = std::make_unique<TransactionStatement>(TransactionStatement::Type::BeginTransaction);
    }
    | COMMIT_TK {
        $$ = std::make_unique<TransactionStatement>(TransactionStatement::Type::CommitTransaction);
    }
    | ABORT_TK {
        $$ = std::make_unique<TransactionStatement>(TransactionStatement::Type::AbortTransaction);
    }
%%

void beedb::parser::Parser::error(const location_type &/*type*/, const std::string& message)
{
    throw exception::ParserException(message);
}
