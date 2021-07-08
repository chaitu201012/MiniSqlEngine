# MiniSqlEngine

A Mini SQL engine coded as part of data systems course.

- The tables are given as .csv files with table name as file name. If the file name is `Table1.csv` then the table name will be `Table1`.
- A `metadata.txt` contains the schema of the database and has the following structure <begin_table> <table_name> . . <end_table>, this will help in identifying the tables that are present in the current relational database.The first line is the name of the table followed by the field or column names.
- Data base is assumed to contain only the integer data.
- Queries are case sensitive as in usual sql engine.
- Only simple queries/subset of queries are supported and the nested queries are not entertained.
- Error handling is done with proper error messages.


## Types of queries supported:

- Normal select queries
  - `select * from table1`
  - `select a, b from table1`
- Aggregate functions like `min, max, avg, sum, count`.
  - `select max(a), min(b) from table1`.
- Select distinct values of a column
  -  `select distinct(a) from table1`.
- Conditional select with at most two conditions joined by `and` or `or`.
  - `select a from table1 where a = 10`.
  - `select a, b from table1 where a = c or b = 5`.
- Table join and aliasing
  - `select * from table1, table2`.
  - `select a, t1.b, c, t2.d from table1 as t1, table2 as t2 where t1.b = t2.b and t2.d >= 0`

- Other complex queries:
  - `select act_id_a , sum(mov_id_am) , count(earnings) from actor , actor_movie , movie  where (act_gender=0 or act_id_a>110) and mov_year>2000 group by act_id_a order by act_id_a;`
  - `select act_id_a , sum(mov_id_am) , count(earnings) from actor , actor_movie , movie  group by act_id_a order by act_id_a desc;`
  - `select distinct act_gender from actor order by act_gender;`

> `In the current implementation I assumed the fields to be unique. As part of further improvement we can add a method to append tablename to the field and parse the queries.`

## How to run the queries:

>python sqlEngine.py "{query}"
>For example: python sqlEngine.py "select * from table1;".


### sample output:
> 1. Select * from table2;
> output: 
table2.B,table2.D
158,11191
773,14421
85,5117
811,13393
311,16116
646,5403
335,6309
803,12262
718,10226
731,13021


> 2.Select A from table1;

> Output:
table1.A
922
640
775
-551
-952
-354
-497
411
-900
858



