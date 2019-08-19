/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System.Diagnostics;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using openCypherTranspiler.Common.Logging;
using openCypherTranspiler.openCypherParser.Common;
using openCypherTranspiler.openCypherParser.AST;
using openCypherTranspiler.CommonTest;
using openCypherTranspiler.Common.Exceptions;

namespace openCypherTranspiler.openCypherParser.Test
{

    /// <summary>
    /// Tests for OpenCypherParser (openCypher ANTLR4 Parser + AST builder)
    /// </summary>
    [TestClass]
    public class OpenCypherParserTest
    {
        private readonly ILoggable _logger = new TestLogger();

        private QueryNode RunQueryAndDumpTree(string cypherQueryText)
        {
            var parser = new OpenCypherParser(_logger);
            var queryNode = parser.Parse(cypherQueryText);
            var tree = queryNode.DumpTree();
            Debug.WriteLine(tree);
            return queryNode;
        }

        [TestMethod]
        public void BasicParsingTest()
        {
            var tree = RunQueryAndDumpTree(@"
MATCH (person:Person)-[r:KNOWS]->(friend:Person)
WITH toFloat(r.score + r.score_delta) AS score2, friend.Name, person.level AS number
WHERE friend.Name='bnitta' OR (person.number >= 1 AND person.number <= 10)
RETURN sum(score2), avg(score2+1) AS scoreShift1
"
            );
            // TODO: structural verifications
            Assert.IsNotNull(tree);
        }

        [TestMethod]
        public void BasicExpressionParsingTest()
        {
            var tree = RunQueryAndDumpTree(@"
MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
WHERE (p.Name = 'Meg Ryan' OR p.Name = 'Tom Hanks') AND m.Title = 'Apollo 13'
RETURN m.Title AS Title, p.Name AS Name
"
            );

            var expressionParsed = tree.GetChildrenOfType<QueryExpressionBinary>();
            Assert.IsNotNull(tree.GetChildrenOfType<QueryExpressionBinary>().Count() == 2);
            Assert.IsNotNull(tree.GetChildrenOfType<QueryExpressionValue>().Any(expr => expr.StringValue == "Meg Ryan"));
            Assert.IsNotNull(tree.GetChildrenOfType<QueryExpressionValue>().Any(expr => expr.StringValue == "Tom Hanks"));
            Assert.IsNotNull(tree.GetChildrenOfType<QueryExpressionValue>().Any(expr => expr.StringValue == "Apollo 13"));
        }

        [TestMethod]
        public void BasicOrderByLimitTest()
        {
            var tree = RunQueryAndDumpTree(@"
MATCH (p:Person)-[:ACTED_IN]->(m:Movie)
WHERE (p.Name = 'Meg Ryan' OR p.Name = 'Tom Hanks') AND m.Title = 'Apollo 13'
RETURN m.Title AS Title, p.Name AS Name
ORDER BY m.Title
LIMIT 5
"
            );

            var expressionParsed = tree.GetChildrenOfType<QueryExpressionBinary>();
            Assert.IsNotNull(tree.GetChildrenOfType<QueryExpressionBinary>().Count() == 2);
            Assert.IsNotNull(tree.GetChildrenOfType<QueryExpressionValue>().Any(expr => expr.StringValue == "Meg Ryan"));
            Assert.IsNotNull(tree.GetChildrenOfType<QueryExpressionValue>().Any(expr => expr.StringValue == "Tom Hanks"));
            Assert.IsNotNull(tree.GetChildrenOfType<QueryExpressionValue>().Any(expr => expr.StringValue == "Apollo 13"));
            Assert.IsNotNull(tree.GetChildrenOfType<SortItem>().Any(c => c.GetChildrenOfType<QueryExpressionProperty>().Any(expr => expr.VariableName == "m" && expr.PropertyName == "Title")));
            Assert.IsNotNull(tree.GetChildrenOfType<LimitClause>().Any(c => c.RowCount == 5));
        }

        [TestMethod]
        public void MatchPatternAdvancedTest()
        {
            {
                // multiple matches
                var tree = RunQueryAndDumpTree(@"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
MATCH (p)-[d:DIRECTED]->(m)
MATCH (p)-[a]->(m2:Movie)
RETURN p.Name, m.Title
"
                );
                // TODO: structural verifications
                Assert.IsNotNull(tree);
            }

            {
                // piped queries
                var tree = RunQueryAndDumpTree(@"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH p, m
MATCH (p)-[d:DIRECTED]->(m)
RETURN p.Name, m.Title
"
                );
                // TODO: structural verifications
                Assert.IsNotNull(tree);
            }

            {
                // optional match
                var tree = RunQueryAndDumpTree(@"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
OPTIONAL MATCH (p)-[d:DIRECTED]->(m)
RETURN p.Name, m.Title
"
                );
                // TODO: structural verifications
                Assert.IsNotNull(tree);
            }

            {
                // match followed by local where
                var tree = RunQueryAndDumpTree(@"
match (p:Person)
where p.Name = 'Tom Hanks'
optional match (p)-[:ACTED_IN]-(m:Movie)
with p, m
where p.Name = 'Tom Hanks'
return p.Name, m.Title
"
                );
                // TODO: structural verifications
                Assert.IsNotNull(tree);
            }

            {
                // optional match case 2: the second subquery is optional
                var tree = RunQueryAndDumpTree(@"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH p, m
OPTIONAL MATCH (p)-[d:DIRECTED]->(m)
RETURN p.Name, m.Title
"
                );
                // TODO: structural verifications
                Assert.IsNotNull(tree);
            }

        }

        [TestMethod]
        public void OperatorTest()
        {
            var tree = RunQueryAndDumpTree(@"
MATCH (p:Person)
WHERE p.Name in ['Tom Hanks', 'Meg Ryan']
RETURN p.Name as Name
"
            );

            Assert.IsNotNull(tree);
            var expressionParsed = tree.GetChildrenOfType<QueryExpressionBinary>();
            Assert.IsTrue(expressionParsed.Any(expr => expr.Operator.Name == BinaryOperator.IN));
            Assert.IsTrue(tree.GetChildrenOfType<QueryExpressionList>().Any(expr => expr.ExpressionList.Count == 2));
        }

        [TestMethod]
        public void StringFunctionTest()
        {
            var tree = RunQueryAndDumpTree(@"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WHERE p.Name STARTS WITH 'Tom' AND (m.Title CONTAINS '13' OR m.Title ENDS WITH 'Mail')
RETURN toUpper(p.Name) as NameCapital, toLower(m.Title) as TitleLC
"
            );

            Assert.IsNotNull(tree);
            Assert.IsTrue(tree.GetChildrenOfType<QueryExpressionFunction>().Count() == 5);
            Assert.IsTrue(tree.GetChildrenOfType<QueryExpressionFunction>().Any(expr => expr.Function.FunctionName == Function.StringContains));
            Assert.IsTrue(tree.GetChildrenOfType<QueryExpressionFunction>().Any(expr => expr.Function.FunctionName == Function.StringStartsWith));
            Assert.IsTrue(tree.GetChildrenOfType<QueryExpressionFunction>().Any(expr => expr.Function.FunctionName == Function.StringEndsWith));
            Assert.IsTrue(tree.GetChildrenOfType<QueryExpressionFunction>().Any(expr => expr.Function.FunctionName == Function.StringToUpper));
            Assert.IsTrue(tree.GetChildrenOfType<QueryExpressionFunction>().Any(expr => expr.Function.FunctionName == Function.StringToLower));
        }

        [TestMethod]
        public void OtherWeirdSyntaxTest()
        {
            { 
                // reuse the same alias in piped query. the second alias is a new alias
                // in below case, m in first match is not the m in second match due to WITH in between
                // the query will return all folks who acted in any movie and directed any movie as well, but
                // not necessarily the same movie
                var tree = RunQueryAndDumpTree(@"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH p, m.Title as ActedMovieTitle
MATCH (p)-[d:DIRECTED]->(m:Movie)
RETURN p.Name, m.Title AS DirectedMovieTitle, ActedMovieTitle
"
                );
                // TODO: structural verifications
                Assert.IsNotNull(tree);
            }
        }

        [TestMethod]
        public void NegativeTest()
        {
            {
                // duplicate returned names
                var expectedExceptionThrown = false;
                try
                {
                    var tree = RunQueryAndDumpTree(@"
MATCH (p:Person)
RETURN p.Name, p.Name
"
                    );
                }
                catch (TranspilerSyntaxErrorException)
                {
                    expectedExceptionThrown = true;
                }
                Assert.IsTrue(expectedExceptionThrown);
            }

            {
                //Node/Edge attached with properties. Not supported
                var expectedExceptionThrown = false;
                try
                {
                    var tree = RunQueryAndDumpTree(@"
MATCH (person:Person {name:'jerrylia'})-[r:KNOWS]->(friend:Person)
WITH toFloat(r.score + r.score_delta) AS score2
WHERE friend.Name='bnitta'
RETURN sum(score2), avg(score2+1) AS scoreShift1
"
                    );
                }
                catch (TranspilerNotSupportedException)
                {
                    expectedExceptionThrown = true;
                }
                Assert.IsTrue(expectedExceptionThrown);
            }

            {
                // match after optional match
                var expectedExceptionThrown = false;
                try
                {
                    var tree = RunQueryAndDumpTree(@"
match (p:Person), (m:Movie)
optional match (p)-[:ACTED_IN]->(m)
match (p)-[:ACTED_IN]->(m)
return p.Name, m.Title
"
                    );
                }
                catch (TranspilerSyntaxErrorException e)
                {
                    Assert.IsTrue(e.Message.Contains("MATCH cannot follow OPTIONAL MATCH"));
                    expectedExceptionThrown = true;
                }
                Assert.IsTrue(expectedExceptionThrown);
            }
        }


        [TestMethod]
        public void CaseExpressionParsingTest()
        {
            var tree = RunQueryAndDumpTree(@"
MATCH (person:Person)-[r:KNOWS]->(friend:Person)
WITH toFloat(r.score + r.score_delta) AS score2, friend.Name, person.level AS number
WHERE friend.Name='bnitta' OR (person.number >= 1 AND person.number <= 10)
RETURN sum(score2), avg(score2+1) AS scoreShift1, 
    case when friend.Name = 'bnitta' then 1 else 0 end as friendFlag
"
            );
            // TODO: structural verifications
            Assert.IsNotNull(tree);
        }
    }
}
