@echo off

del MovieDBSQLCmds.sql

echo IF db_id('octestdb') is null CREATE DATABASE octestdb; > MovieDBSQLCmds.sql
echo GO >> MovieDBSQLCmds.sql
echo >> MovieDBSQLCmds.sql
echo USE octestdb; >> MovieDBSQLCmds.sql
echo GO >> MovieDBSQLCmds.sql
echo >> MovieDBSQLCmds.sql

CALL :CONVERT_SQL person.csv Person MovieDBSQLCmds.sql
CALL :CONVERT_SQL movie.csv Movie MovieDBSQLCmds.sql
CALL :CONVERT_SQL acted_in.csv ActedIn MovieDBSQLCmds.sql
CALL :CONVERT_SQL produced.csv Produced MovieDBSQLCmds.sql
CALL :CONVERT_SQL directed.csv Directed MovieDBSQLCmds.sql
CALL :CONVERT_SQL wrote.csv Wrote MovieDBSQLCmds.sql
CALL :CONVERT_SQL reviewed.csv Reviewed MovieDBSQLCmds.sql
CALL :CONVERT_SQL follows.csv Follows MovieDBSQLCmds.sql

GOTO :EOF

REM %1 is CSV file
REM %2 is the table name
REM %3 is the resulting SQL file
:CONVERT_SQL
echo python ConvertToSQLCmds.py %1 true %2 %3
python ConvertToSQLCmds.py %1 true %2 %3
 
GOTO :EOF
 
:EOF