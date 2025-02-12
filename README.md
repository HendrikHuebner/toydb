

SQL subset grammar

```

<program> ::= <statement>+
<statement> ::= <insert_statement> | <select_statement> | <update_statement> | <delete_statement>

<insert_statement> ::= "INSERT INTO" <table_name> "(" <columns> ")" "VALUES" "(" <values> ")"
<table_name> ::= IDENTIFIER
<columns> ::= IDENTIFIER ("," IDENTIFIER)*
<values> ::= <value> ("," <value>)*
<value> ::= STRING | NUMBER | BOOLEAN | NULL

<select_statement> ::= "SELECT" <select_columns> "FROM" <table_name> <where_condition>? <join_clause>* <order_by>?
<select_columns> ::= "*" | IDENTIFIER ("," IDENTIFIER)*
<where_condition> ::= "WHERE" <condition>
<condition> ::= <expression> ("AND" | "OR" <expression>)*
<expression> ::= IDENTIFIER <comparator> <value>
<comparator> ::= "=" | ">" | "<" | ">=" | "<=" | "!="
<join_clause> ::= "JOIN" <table_name> "ON" <join_condition>
<join_condition> ::= IDENTIFIER <comparator> IDENTIFIER
<order_by> ::= "ORDER BY" IDENTIFIER

<update_statement> ::= "UPDATE" <table_name> "SET" <set_clauses> <where_condition>
<set_clauses> ::= IDENTIFIER "=" <value> ("," IDENTIFIER "=" <value>)*
<delete_statement> ::= "DELETE FROM" <table_name> <where_condition>

<value> ::= STRING | NUMBER | BOOLEAN | NULL
<IDENTIFIER> ::= [a-zA-Z_][a-zA-Z0-9_]*

```