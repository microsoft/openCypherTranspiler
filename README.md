# openCypher Transpiler


This library help you to build an [openCypher](http://www.opencypher.org/) query layer on top of a relational database or structured data in data lakes. Built on top of this library, you can transpile openCypher query into a target query language used by the relational database. We have provided a sample target language renderer for [T-SQL](https://docs.microsoft.com/en-us/sql/t-sql/language-reference?view=sql-server-2017).

Originally we built this library to provide a [Property Graph](https://neo4j.com/developer/graph-database/#property-graph) data-layer for the petabyte-scale data assets in the Azure Data Lake. The property graph enables us to organize a vast catalog of data in an intuitive and traceable way. The openCypher query language allows data users that are already familiar with SQL-alike declarative query language to query the data assets with ease: in a graph where the relationship between data entities is a first class citizen, the data users no longer need to spend time on figuring out how individual data assets should be joined together. The data producers who are experts on their data would be the one to help define the relationship and is able to ensure its quality for downstream consumptions.

This library has three main components:

* A openCypher parser built on top of ANTLR4 and official openCypher grammar to parse and create an [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree) to abstract the syntactical structure of the graph query;
* A logic planner transforms the AST into relational query logical plan similar to the [Relational Algebra](https://en.wikipedia.org/wiki/Relational_algebra);
* A query code renderer produces the actual query code from the logical plan. In this repository, we provides a T-SQL renderer.

The library, written in [.Net Core](https://dotnet.microsoft.com/download), is cross-platform.


## Using the library

```CSharp
// To Be Provided
```


## Build on top of this library

```CSharp
// To Be Provided
```


## Test designs

Transpiler is tested using the T-SQL target renderer and its results are compared against what is produced by Cypher from the [Neo4j Graph Database](https://neo4j.com/graph-database). Each test compares the query results from the transpiled query on [Microsoft SQL for Linux](https://www.microsoft.com/en-us/sql-server/sql-server-2017) against Cypher on Neo4j 3.x to ensure they are consistent in term of data type and contents.

To run the tests, simply run under the project root folder:
```batch
dotnet test
```


## Limitations To Be Addressed

* MATCH pattern with unspecified labels which cannot be implied to a single label/relationship type
* MATCH pattern with variable-length relationship
* The logical plan is currently not further optimized with assumption the underlying RDBMS engine does this work
* list,collect,UNWIND is currently not supported
* Source table must already been normalized to be used as graph node/edge
* Readonly query is supported. CALL and write (such as MERGE) is not supported.


## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
