# openCypherParser

The 'Grammar' sub-folder contains [ANTLR4](https://github.com/antlr) generated visitors using the g4 grammar file published by [openCypher project](http://www.opencypher.org/). 

To regenerate the grammar file in the event of openCypher specification updates, please follow the steps below.

## Prerequisites

Before building project, make sure you have:
* Java 8 SDK. E.g. on Windows
* Dotnet Core SDK 2.2
* Latest ANTLR4 Tools (download and use [ANTLR4 tools](https://github.com/antlr/antlr4/blob/master/doc/getting-started.md))

E.g. on Windows:

```batch
REM Download and install dependencies
choco install dotnetcore-sdk openjdk curl

REM Get ANTLR4 tools
mkdir %USERPROFILE%\javalibs
cd /d %USERPROFILE%\javalibs
curl https://www.antlr.org/download/antlr-4.7.2-complete.jar --output antlr-4.7.2-complete.jar
SET CLASSPATH=%CLASSPATH%%USERPROFILE%\javalibs\antlr-4.7.2-complete.jar;
setx CLASSPATH %CLASSPATH%
```

## (Re)Build Cypher visitor base

To rebuild the grammar classes do:

```batch
cd <ProjectFolder>\openCypherParser\Grammar
antlr4 -package openCypherTranspiler.openCypherParser.ANTLR -Dlanguage=CSharp -no-listener -visitor Cypher.g4
```
