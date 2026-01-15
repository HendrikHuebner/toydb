

SQL subset grammar

Not fully implemented in the parser yet. Missing:

- aliases
- char type
- join syntax
- varchar type (instead of fixed size string?)
- date type
- non inner joins

```

<program> ::= <statement>+
<statement> ::= <insert_statement> | <select_statement> | <update_statement> | <delete_statement> | <create_table_statement>

<create_table_statement> ::= "CREATE TABLE" <table_name> "(" <column_definitions> ")"
<column_definitions> ::= <column_definition> ("," <column_definition>)*
<column_definition> ::= IDENTIFIER <data_type>
<data_type> ::= "INTEGER" | "BIGINT" | "DOUBLE" | "BOOL" | "STRING"

<insert_statement> ::= "INSERT INTO" <table_name> ("(" <columns> ")")? "VALUES" "(" <values> ")" ("," "(" <values> ")")*
<table_name> ::= IDENTIFIER ("AS" IDENTIFIER)?
<columns> ::= IDENTIFIER ("," IDENTIFIER)*
<values> ::= <value> ("," <value>)*
<value> ::= STRING | INT32 | INT64 | DOUBLE | BOOLEAN | NULL

<select_statement> ::= "SELECT" <select_columns> "FROM" <table_list> <where_condition>? <order_by>?
<select_columns> ::= "*" | <select_column> ("," <select_column>)*
<select_column> ::= <qualified_column> ("AS" IDENTIFIER)?
<qualified_column> ::= IDENTIFIER | IDENTIFIER "." IDENTIFIER
<table_list> ::= <table_name> ("," <table_name>)*
<where_condition> ::= "WHERE" <condition>
<condition> ::= <expression> (("AND" | "OR") <expression>)*
<expression> ::= <qualified_column> <comparator> <value> | "(" <condition> ")"
<comparator> ::= "=" | ">" | "<" | ">=" | "<=" | "!="
<order_by> ::= "ORDER BY" <qualified_column>

<update_statement> ::= "UPDATE" <table_name> "SET" <set_clauses> <where_condition>
<set_clauses> ::= IDENTIFIER "=" <value> ("," IDENTIFIER "=" <value>)*
<delete_statement> ::= "DELETE FROM" <table_name> <where_condition>

<value> ::= STRING | INT32 | INT64 | DOUBLE | BOOLEAN | NULL
<IDENTIFIER> ::= [a-zA-Z_][a-zA-Z0-9_]*

```