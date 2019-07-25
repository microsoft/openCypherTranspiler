set NEODB=https://jerrylia-home-2-wifi:7473
set USER=neo4j
set PW=neo4jtest
Setlocal

CALL :DOWNLOAD_AS_CSV "match (p:Person) return p.id as id, p.Name as Name, p.Born as Born" person.csv
CALL :DOWNLOAD_AS_CSV "match (m:Movie) return m.id as id, m.Title as Title, m.Tagline as Tagline, m.Released as Released" movie.csv

CALL :DOWNLOAD_AS_CSV_CUSTOMJQ "match (m:Person)-[r:ACTED_IN]->(n:Movie) return m.id as _vertexId, n.id as _sink, r.Roles as Roles" acted_in.csv acted_in.jq.txt
CALL :DOWNLOAD_AS_CSV "match (m:Person)-[r:PRODUCED]->(n:Movie) return m.id as _vertexId, n.id as _sink" produced.csv
CALL :DOWNLOAD_AS_CSV "match (m:Person)-[r:DIRECTED]->(n:Movie) return m.id as _vertexId, n.id as _sink" directed.csv
CALL :DOWNLOAD_AS_CSV "match (m:Person)-[r:WROTE]->(n:Movie) return m.id as _vertexId, n.id as _sink" wrote.csv
CALL :DOWNLOAD_AS_CSV "match (m:Person)-[r:REVIEWED]->(n:Movie) return m.id as _vertexId, n.id as _sink, r.Summary as Summary, r.Rating as Rating" reviewed.csv
CALL :DOWNLOAD_AS_CSV "match (m:Person)-[r:FOLLOWS]->(n:Person) return m.id as _vertexId, n.id as _sink" follows.csv
 
goto :EOF

REM %1 is the cypher command
REM %2 is the output file
:DOWNLOAD_AS_CSV
echo {"statements":[{"statement":"%~1"}]} > dn.req.temp.txt
curl --insecure -u %USER%:%PW% ^
  -H accept:application/json -H content-type:application/json ^
  -d @dn.req.temp.txt ^
  %NEODB%/db/data/transaction/commit ^
  | jq -r "(.results[0]) | .columns,.data[].row | @csv" ^
  > %~2
 
 GOTO:EOF
 
REM %1 is the cypher command
REM %2 is the output file
REM %3 is the JQ command file when the command doesn't work well over command line interface
 :DOWNLOAD_AS_CSV_CUSTOMJQ
echo {"statements":[{"statement":"%~1"}]} > dn.req.temp.txt
curl --insecure -u %USER%:%PW% ^
  -H accept:application/json -H content-type:application/json ^
  -d @dn.req.temp.txt ^
  %NEODB%/db/data/transaction/commit ^
  | jq -f %~3 -r ^
  > %~2
 
 GOTO:EOF
  
:EOF