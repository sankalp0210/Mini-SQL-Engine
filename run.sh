python3 *.py "select * from table1;"

python3 *.py "select * from table1, table2;"

python3 *.py "select A, table1.B from table1, table2;"

python3 *.py "select table1.B, table2.D from table1, table2;"

python3 *.py "select distinct B from table1;"

python3 *.py "select A, table2.B from table1, table2 where table1.A > 900 AND table2.D < 200;"

python3 *.py "select * from table1, table2 where table1.B > table2.D;"

python3 *.py "select A, table1.B, D from table1, table2 where table1.B = table2.D;"

python3 *.py "select A, B, C from table1, table2;"

python3 *.py "select distinct A, D from table1, table2;"

python3 *.py "select distinct A, B from table1, table2;"

python3 *.py "Select max(A) from table1, table2 where table1.A = table2.D;"

python3 *.py "select A, table2.B from table1, table2 where table1.A > 900 AND table2.B < 100;"

python3 *.py "select A, table2.B from table1, table2 where table1.A > 900 AND table2.B = 311;"

python3 *.py "Select table1.B,table2.B from table1,table2 where table1.B=table2.B;"

python3 *.py "Select table1.B, table2.B from table1, table2 where A > 0 and table1.B = table2.B;"
