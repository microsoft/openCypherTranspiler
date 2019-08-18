/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.Common.GraphSchema;
using openCypherTranspiler.Common.Logging;
using openCypherTranspiler.CommonTest;
using openCypherTranspiler.LogicalPlanner.Utils;
using openCypherTranspiler.openCypherParser;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;
using JT = openCypherTranspiler.LogicalPlanner.JoinOperator.JoinType;

namespace openCypherTranspiler.LogicalPlanner.Test
{
    /// <summary>
    /// Test the logical planner (which turns parsed cypher statement into
    /// relational algebra
    /// </summary>
    [TestClass]
    public class LogicalPlannerTest
    {
        private ILoggable _logger = new TestLogger();

        private bool MatrixEquals<T>(T[,] data1, T[,] data2)
        {
            var equal =
                data1.Rank == data2.Rank &&
                Enumerable.Range(0, data1.Rank).All(dimension => data1.GetLength(dimension) == data2.GetLength(dimension)) &&
                data1.Cast<T>().SequenceEqual(data2.Cast<T>());

            return equal;
        }

        private LogicalPlan RunQueryAndDumpTree(IGraphSchemaProvider graphDef, string cypherQueryText)
        {
            var parser = new OpenCypherParser(_logger);
            var queryNode = parser.Parse(cypherQueryText);
            var planner = LogicalPlan.ProcessQueryTree(queryNode, graphDef);

            Assert.IsNotNull(planner.StartingOperators);
            Assert.IsNotNull(planner.TerminalOperators);

            // Dump both parse tree and logical plan
            var tree = queryNode.DumpTree();
            var logical = planner.DumpGraph();

            return planner;
        }

        [TestMethod]
        public void TestTransitiveClosureHelper()
        {
            {
                // equivalent of MATCH (a)-[b]-(c), (d)
                var startMat = new JT[4, 4]
                {
                    { JT.Inner, JT.Inner, JT.Cross, JT.Cross },
                    { JT.Inner, JT.Inner, JT.Inner, JT.Cross },
                    { JT.Cross, JT.Inner, JT.Inner, JT.Cross },
                    { JT.Cross, JT.Cross, JT.Cross, JT.Inner },
                };

                var expectedMat = new JT[4, 4]
                {
                    { JT.Inner, JT.Inner, JT.Inner, JT.Cross },
                    { JT.Inner, JT.Inner, JT.Inner, JT.Cross },
                    { JT.Inner, JT.Inner, JT.Inner, JT.Cross },
                    { JT.Cross, JT.Cross, JT.Cross, JT.Inner },
                };

                var actualMat = startMat.TransitiveClosure();
                Assert.IsTrue(MatrixEquals(expectedMat, actualMat));
            }

            {
                // equivalent of MATCH (a)-[b]-(c), 
                //               OPTIONAL MATCH (c)-[d]-(e)
                var startMat = new JT[5, 5]
                {
                    { JT.Inner, JT.Inner, JT.Cross, JT.Cross, JT.Cross },
                    { JT.Inner, JT.Inner, JT.Inner, JT.Cross, JT.Cross },
                    { JT.Cross, JT.Inner, JT.Inner, JT.Left,  JT.Cross },
                    { JT.Cross, JT.Cross, JT.Left,  JT.Inner, JT.Left  },
                    { JT.Cross, JT.Cross, JT.Cross, JT.Left,  JT.Inner },
                };

                var expectedMat = new JT[5, 5]
                {
                    { JT.Inner, JT.Inner, JT.Inner, JT.Left,  JT.Left  },
                    { JT.Inner, JT.Inner, JT.Inner, JT.Left,  JT.Left  },
                    { JT.Inner, JT.Inner, JT.Inner, JT.Left,  JT.Left  },
                    { JT.Left,  JT.Left,  JT.Left,  JT.Inner, JT.Left  },
                    { JT.Left,  JT.Left,  JT.Left,  JT.Left,  JT.Inner },
                };

                var actualMat = startMat.TransitiveClosure();
                Assert.IsTrue(MatrixEquals(expectedMat, actualMat));
            }

            {
                // equivalent of MATCH (a)-[b]-(c), 
                //               MATCH (c)-[d]-(a)
                //               OPTIONAL MATCH (c)-[e]-(a)
                var startMat = new JT[5, 5]
                {
                    { JT.Inner, JT.Inner, JT.Cross, JT.Inner, JT.Left  },
                    { JT.Inner, JT.Inner, JT.Inner, JT.Cross, JT.Cross },
                    { JT.Cross, JT.Inner, JT.Inner, JT.Inner, JT.Left  },
                    { JT.Inner, JT.Cross, JT.Inner, JT.Inner, JT.Cross },
                    { JT.Left,  JT.Cross, JT.Left,  JT.Cross, JT.Inner },
                };

                var expectedMat = new JT[5, 5]
                {
                    { JT.Inner, JT.Inner, JT.Inner, JT.Inner, JT.Left  },
                    { JT.Inner, JT.Inner, JT.Inner, JT.Inner, JT.Left  },
                    { JT.Inner, JT.Inner, JT.Inner, JT.Inner, JT.Left  },
                    { JT.Inner, JT.Inner, JT.Inner, JT.Inner, JT.Left  },
                    { JT.Left,  JT.Left,  JT.Left,  JT.Left,  JT.Inner },
                };

                var actualMat = startMat.TransitiveClosure();
                Assert.IsTrue(MatrixEquals(expectedMat, actualMat));
            }
        }

        [TestMethod]
        public void BasicTestLogicalPlanner()
        {
            IGraphSchemaProvider graphDef = new JSONGraphSchema(@"./TestData/MovieGraph.json");

            // Basic test with no WITH and OPTIONAL
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
RETURN p.Name, m.Title
"
                );
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<JoinOperator>().All(o => o.Type == JT.Inner));
            }

            // Basic test with optional match
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[r:REVIEWED]->(m:Movie)
OPTIONAL MATCH (p)<-[:FOLLOWS]-(p2:Person)-[r2:REVIEWED]->(m)
return p.Name as Name1, p2.Name as Name2, m.Title as Title, r.Rating as Rating1, r2.Rating as Rating2
"
                );
                // TODO: structural verification
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<JoinOperator>().Any(o => o.Type == JT.Left));
                // nullable no change after optional match
                Assert.IsTrue((lp.TerminalOperators.First().OutputSchema.First(f => f.FieldAlias == "Name1") as ValueField).FieldType == typeof(string));
                Assert.IsTrue((lp.TerminalOperators.First().OutputSchema.First(f => f.FieldAlias == "Name2") as ValueField).FieldType == typeof(string));
                // non nullable changed to nullable after optional match
                Assert.IsTrue((lp.TerminalOperators.First().OutputSchema.First(f => f.FieldAlias == "Rating1") as ValueField).FieldType == typeof(int));
                Assert.IsTrue((lp.TerminalOperators.First().OutputSchema.First(f => f.FieldAlias == "Rating2") as ValueField).FieldType == typeof(int?));
            }

            // Basic test with WITH and OPTIONAL match
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH p, m
OPTIONAL MATCH (p)-[d:DIRECTED]->(m)
RETURN p.Name, m.Title
"
                );
                // TODO: structural verification
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<JoinOperator>().Any(o => o.Type == JT.Left));
            }

            // Duplicated match patterns
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie), (p)-[p2:PRODUCED]->(m)
MATCH (p)-[p2:PRODUCED]->(m)
OPTIONAL MATCH (p)-[d:DIRECTED]->(m)
RETURN p.Name, m.Title
"
                );
                // TODO: structural verification for duplication removal
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<JoinOperator>().Any(o => o.Type == JT.Left));
            }

            // Optional match follows WITH
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie), (p)-[p2:PRODUCED]->(m)
WITH a.Roles as ActedRoles, p, m
OPTIONAL MATCH (p)-[d:REVIEWED]->(m)
WITH d.Summary as ReviewSummary, d.Rating as ReviewRating, p, m, ActedRoles
RETURN p.Name, m.Title, ActedRoles, ReviewRating, ReviewSummary
"
                );
                // TODO: structural verification
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<JoinOperator>().Any(o => o.Type == JT.Left));
            }

            // Twisted alias for with/return entities
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH p as m, m as p
MATCH (m)-[a:DIRECTED]->(p)
RETURN m.Name, p.Title
"
                );
                // TODO: structural verification
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
            }

            // Join two already joined patterns
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[:REVIEWED]->(m:Movie)
MATCH (p2:Person)-[:REVIEWED]->(m2:Movie)
MATCH (p:Person)-[:FOLLOWS]->(p2:Person)
RETURN p.Name as Name1, m.Title as Title1, p2.Name as Name2, m2.Title as Title2
"
                );
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<JoinOperator>().Any(o => o.Type == JT.Inner));
            }

            // Cross Join
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person), (m:Movie)
RETURN p.Name as Name1, m.Title as Title1
"
                );
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<JoinOperator>().Any(o => o.Type == JT.Cross));
            }

            // basic WHERE test
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[:REVIEWED]->(m:Movie)
WHERE p.Name = 'Tom Hanks'
RETURN p.Name as Name
"
                );
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<SelectionOperator>().Any(o => o.FilterExpression != null));
            }

            // WHERE attached to optional match
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)
OPTIONAL MATCH (p:Person)-[:REVIEWED]->(m:Movie)
WHERE p.Name = 'Tom Hanks'
RETURN p.Name as Name
"
                );
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<JoinOperator>().Any(o => o.Type == JT.Left));
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<LogicalOperator>().Any(o => o.OutOperators?.Count() == 2));
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<SelectionOperator>().Any(o => o.FilterExpression != null));
            }

            // WHERE attached to WITH projection
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)
OPTIONAL MATCH (p:Person)-[:REVIEWED]->(m:Movie)
WITH p, m
WHERE p.Name = 'Tom Hanks'
RETURN p.Name as Name
"
                );
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<JoinOperator>().Any(o => o.Type == JT.Left));
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<LogicalOperator>().All(o => (o.OutOperators?.Count() ?? 0) <= 1));
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<SelectionOperator>().Any(o => o.FilterExpression != null));
            }

            // LIMIT only attached to RETURN
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)
RETURN p.Name as Name
LIMIT 5
"
                );
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<SelectionOperator>().Any(o => (o.Limit?.RowCount ?? 0) == 5));
            }

            // ORDER BY only attached to RETURN
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)
RETURN p.Name as Name2
ORDER BY Name2
"
                );
                // TODO: structural verification
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<SelectionOperator>().Any(o => (o.OrderByExpressions?.Count() ?? 0) == 1));
            }

            // ORDER BY only attached to RETURN with implicit field reference
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)
RETURN p.Name as Name
ORDER BY p.id
"
                );
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<SelectionOperator>().Any(o => (o.OrderByExpressions?.Count() ?? 0) == 1));
            }

            // ORDER BY + Limit to RETURN
            {
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)
RETURN p.Name as Name
ORDER BY p.Name LIMIT 3
"
                );
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<SelectionOperator>().Any(o => (o.OrderByExpressions?.Count() ?? 0) == 1));
                Assert.IsTrue(lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<SelectionOperator>().Any(o => (o.Limit?.RowCount ?? 0) == 3));
            }
        }

        [TestMethod]
        public void AdvancedTestLogicalPlanner()
        {
            IGraphSchemaProvider graphDef = new JSONGraphSchema(@"./TestData/MovieGraph.json");

            // Duplicate (or overlapping) match pattern across WITH barrier
            {
                // join is outter then upgraded to inner after WITH barrier
                // Today, we have less optimized plan by not allow the join
                // to propagate up past the WITH barrier.
                var lp = RunQueryAndDumpTree(graphDef, @"
match (n: Person), (m: Movie)
where n.Name = 'Tom Hanks'
with n, m
optional match(n)-[:ACTED_IN]-(m)
with n, m
match(n)-[:ACTED_IN]-(m)
return distinct n.Name, m.Title
"
                );
                // TODO: structural verification
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
            }

            // More complicated pattern in a single match clause that requires inequality conditions to be added
            {
                // implicit condition to be added is ACTED_IN should not be the same by its 2 key ids
                var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(p2:Person)
RETURN p.Name AS Name, p2.Name as CoStarName
"
                );
                Assert.IsTrue(lp.StartingOperators.Count() > 0);
                Assert.IsTrue(lp.TerminalOperators.Count() == 1);
                // make sure that the selection condition is added
                var conditionOpsAdded = lp.TerminalOperators.First().GetAllUpstreamOperatorsOfType<SelectionOperator>();
                Assert.IsTrue(conditionOpsAdded.Count() == 1);
                Assert.IsTrue(conditionOpsAdded.First().UnexpandedEntityInequalityConditions.Count() == 1);
                Assert.IsTrue(conditionOpsAdded.First().FilterExpression != null);
            }
        }

        public void NegativeTestLogicalPlanner()
        {
            IGraphSchemaProvider graphDef = new JSONGraphSchema(@".\TestData\Movie\MovieGraph.json");

            // Our implementation block of returning whole entity instead of its fields
            // We don't support packing whole entity into JSON today, as JSON blob in Cosmos
            // is not very useful for any further computations
            {
                try
                {
                    var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie), (p)-[p2:PRODUCED]->(m)
MATCH (p)-[p2:PRODUCED]->(m)
OPTIONAL MATCH (p)-[d:DIRECTED]->(m)
RETURN p
"
                    );
                    Assert.Fail("Didn't failed as expected.");
                }
                catch (TranspilerNotSupportedException e)
                {
                    Assert.IsTrue(e.Message.Contains("Query final return body returns the whole entity"));
                }
            }

            // repeated relationship pattern on the same match statement
            // NOTE: repeated relationship pattern not on the same match statement is still supported
            {
                try
                {
                    var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie), (p2:Person)-[a:ACTED_IN]->(m2:Movie)
RETURN p.Name, m.Title
"
                    );
                    Assert.Fail("Didn't failed as expected.");
                }
                catch (TranspilerNotSupportedException e)
                {
                    Assert.IsTrue(e.Message.Contains("Cannot use the same relationship variable 'a'"));
                }

            }
        }

        public void NegativeTestLogicalPlannerSchemaBinding()
        {
            IGraphSchemaProvider graphDef = new JSONGraphSchema(@"./TestData/MovieGraph.json");

            // Binding to non exist node
            {
                try
                {
                    var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Actor)
RETURN p
"
                    );
                    Assert.Fail("Didn't failed as expected.");
                }
                catch (TranspilerBindingException e)
                {
                    Assert.IsTrue(e.Message.Contains("Actor"));
                }

                try
                {
                    var lp = RunQueryAndDumpTree(graphDef, @"
MATCH (p:Person)-[:Performed]-(m:Movie)
RETURN p
"
                    );
                    Assert.Fail("Didn't failed as expected.");
                }
                catch (TranspilerBindingException e)
                {
                    Assert.IsTrue(e.Message.Contains("Performed"));
                }
            }
        }
    }
}
