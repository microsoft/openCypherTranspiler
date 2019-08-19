/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.IO;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Neo4j.Driver.V1;
using Docker.DotNet;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Threading;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.Common.Logging;
using openCypherTranspiler.CommonTest;
using openCypherTranspiler.openCypherParser;
using openCypherTranspiler.LogicalPlanner;


namespace openCypherTranspiler.SQLRenderer.Test
{
    /// <summary>
    /// Tests for SQLRenderer
    /// 
    /// References:
    /// - Docker.DotNet how to use PortBindings of HostConfig:
    ///   https://github.com/Microsoft/Docker-PowerShell/issues/174
    /// - Docker REST API doc:
    ///   https://docs.docker.com/engine/api/v1.39/
    /// - appsettings:
    ///   https://blog.bitscry.com/2017/05/30/appsettings-json-in-net-core-console-app/
    ///   https://www.humankode.com/asp-net-core/asp-net-core-configuration-best-practices-for-keeping-secrets-out-of-source-control
    /// </summary>
    [TestClass]
    public class SQLRendererTest
    {
        private ISQLDBSchemaProvider _graphDef;
        private readonly ILoggable _logger = new TestLogger(LoggingLevel.Normal);

        private IDriver _driver;
        private Func<SqlConnection> _conn;
        private Func<SqlConnection> _conn_init;

        #region Test initialization and clean up

        //
        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext) { }

        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        public static void MyClassCleanup() { }

        //
        // Use TestInitialize to run code before running each test 
        [TestInitialize()]
        public void TestInitialize()
        {
            // load graph definition and connect to the servers
            _graphDef = new JSONGraphSQLSchema(@"./TestData/MovieGraph.json");

            // setup neo4j and sql drivers
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("./appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile("./appsettings.private.json", optional: true, reloadOnChange: true);
            IConfigurationRoot configuration = builder.Build();

            var useDocker = bool.Parse(configuration
                .GetSection("appSettings")
                .GetSection("createLocalDockerImages").Value);
            var neo4jHost = configuration
                .GetSection("appSettings")
                .GetSection("neo4jHost").Value;
            var neo4jPort = int.Parse(configuration
                .GetSection("appSettings")
                .GetSection("neo4jPort").Value);
            var neo4jUser = configuration
                .GetSection("appSettings")
                .GetSection("neo4jUser").Value;
            var neo4jPassword = configuration
                .GetSection("appSettings")
                .GetSection("neo4jPassword").Value;
            var sqlHost = configuration
                .GetSection("appSettings")
                .GetSection("sqlHost").Value;
            var sqlPort = int.Parse(configuration
                .GetSection("appSettings")
                .GetSection("sqlPort").Value);
            var sqlPassword = configuration
                .GetSection("appSettings")
                .GetSection("sqlPassword").Value;
            var skipInitialization = string.Compare(configuration
                .GetSection("appSettings")
                .GetSection("skipInitialization").Value, "true", true) == 0;

            var neo4jBoltUrl = $"bolt://{neo4jHost}:{neo4jPort}";
            var sqlConnStrInit = $"Data Source={sqlHost},{sqlPort};User id=SA;Password={sqlPassword};";
            var sqlConnStr = $"Data Source={sqlHost},{sqlPort};Initial Catalog=octestdb;User id=SA;Password={sqlPassword};";

            _driver = GraphDatabase.Driver(neo4jBoltUrl, AuthTokens.Basic(neo4jUser, neo4jPassword));
            _conn = () => new SqlConnection(sqlConnStr);
            _conn_init = () => new SqlConnection(sqlConnStrInit);

            // initialize test harness
            if (!File.Exists("./TestInitDone.tmp") || !skipInitialization)
            {
                if (useDocker)
                {
                    // create and start test harness containers containers
                    CreateTestHarnessContainers(
                        neo4jUser,
                        neo4jPassword,
                        neo4jPort,
                        sqlHost,
                        sqlPort,
                        sqlPassword
                        ).ConfigureAwait(false).GetAwaiter().GetResult();
                }

                // initialize neo4j db
                InitializeNeo4jDB();

                // initialize sql db
                InitializeSQLDB();

                Console.WriteLine("Test Initialization completed.");
                File.WriteAllText("./TestInitDone.tmp", DateTime.UtcNow.ToString());
            }
            else
            {
                // Depending the cached the initialization state to speed up unit test
                Console.WriteLine("Test Initialization skipped. To redo test initialization, remove './TestInitDone.tmp'.");
            }
        }

        #endregion Test initialization and clean up

        private async Task CreateTestHarnessContainers(
            string neo4jUser,
            string neo4jPassword,
            int neo4jBoltPort,
            string sqlHost,
            int sqlPort,
            string sqlPassword
            )
        {
            var dockerClient = new DockerClientConfiguration(
                RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ?
                    new Uri("npipe://./pipe/docker_engine") :
                    new Uri("unix:///var/run/docker.sock")
            ).CreateClient();

            // start Neo4j test instance
            // Bolt: 10087
            // HTTP: 10074
            await DockerContainer.On(dockerClient, _logger)
                .UseImage("neo4j")
                .SetName("neo4j_unittest_1")
                //.AddTcpPort(7474, 10074)
                .AddTcpPort(7687, neo4jBoltPort)
                .AddEnv("NEO4J_AUTH", $"{neo4jUser}/{neo4jPassword}")
                .Up(rebuildIfExists: true, waitForAllTcpPorts: true);

            // start Microsoft SQL test instance
            await DockerContainer.On(dockerClient, _logger)
                .UseImage("mcr.microsoft.com/mssql/server", "2017-latest")
                .SetName("mssql_unittest_1")
                .AddTcpPort(1433, sqlPort)
                .AddEnv("ACCEPT_EULA", "y")
                .AddEnv("SA_PASSWORD", sqlPassword)
                .Up(rebuildIfExists: true, waitForAllTcpPorts: true);
        }

        private void InitializeNeo4jDB()
        {
            using (var session = _driver.Session())
            {
                session.Run("MATCH (n) DETACH DELETE n");
                var movieDbCreationScript = File.ReadAllText(@"./TestData/MovieGraphNeo4jQuery.txt");
                session.Run(movieDbCreationScript);
            }
        }

        private void WaitForSQLToInitialize()
        {
            var cmdText = "USE octestdb;";
            var waitInterval = 5000;
            var waitIntervalMax = 180000;
            var backoutFactor = 2;
            var retriesMax = 5;
            int retry = 0;
            while(retry < retriesMax)
            {
                try
                {
                    using (var con = _conn_init())
                    {
                        con.Open();
                        using (var command = new SqlCommand(cmdText, con))
                        {
                            command.ExecuteNonQuery();
                        }
                        con.Close();
                    }
                    break;
                }
                catch (SqlException)
                {
                    if (retry < retriesMax)
                    {
                        int retryTime = Math.Min((int)(waitInterval * Math.Pow(backoutFactor, retry)), waitIntervalMax);
                        Thread.Sleep(retryTime);
                        _logger?.Log($"SQL Server is not ready yet, retry in {retryTime/1000} seconds ...");
                        retry++;
                        continue;
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        private void InitializeSQLDB()
        {
            var movieDbCreationScript = File.ReadAllLines(@"./TestData/MovieDBSQLCmds.sql");
            var goLines = movieDbCreationScript.Select(
                (l, i) => string.Compare(l.Trim(), "go", true) == 0 || string.Compare(l.Trim(), "go;", true) == 0 ?
                    i : -1
                ).Where(idx => idx >= 0)
                .Union(new List<int>() { movieDbCreationScript.Length });

            WaitForSQLToInitialize();

            using (var con = _conn_init())
            {
                con.Open();
                var startIdx = 0;
                foreach (var endIdx in goLines)
                {
                    var cmdText = string.Join("\r\n", movieDbCreationScript.Skip(startIdx).Take(endIdx - startIdx));
                    if (!string.IsNullOrWhiteSpace(cmdText.Trim()))
                    { 
                        using (var command = new SqlCommand(cmdText, con))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                    startIdx = endIdx + 1;
                }
                con.Close();
            }
        }

        private DataTable RunQueryInCypherAndGetResult(string cypherQueryText)
        {
            var dataTable = new DataTable();
            var typeOrder = new Type[] { typeof(bool), typeof(byte), typeof(char), typeof(int), typeof(DateTime), typeof(long), typeof(double), typeof(string), typeof(object) };
            var typeOrderDict = typeOrder.Select((t, i) => (t:t, i:i)).ToDictionary(kv => kv.t, kv => kv.i);
            using (var session = _driver.Session())
            {
                var result = session.Run(cypherQueryText).ToList();
                var resultFirstRec = result.FirstOrDefault();

                // get type for each column
                if (resultFirstRec != null)
                {
                    // try to find most generous type that can accommodate the data
                    var colTypes = new Type[resultFirstRec.Keys.Count];
                    foreach (var record in result)
                    {
                        foreach (var v in record.Values.Select((v, i) => (v:v, i:i)))
                        {
                            colTypes[v.i] = v.v.Value != null ?
                                typeOrder[Math.Max(typeOrderDict[v.v.Value.GetType()], typeOrderDict[colTypes[v.i] ?? typeOrder.First()])] :
                                colTypes[v.i];
                        };
                    }
                    foreach (var col in resultFirstRec.Keys.Select((v, i) => (v:v, i:i)))
                    {
                        dataTable.Columns.Add(col.v, colTypes[col.i] ?? typeof(object));
                    }
                    foreach (var record in result)
                    {
                        var dataRow = dataTable.NewRow();
                        foreach (var v in record.Values)
                        {
                            dataRow[v.Key] = v.Value ?? DBNull.Value;
                        };
                        dataTable.Rows.Add(dataRow);
                    }
                }
            }
            return dataTable;
        }

        private DataTable RunQueryInSQLAndGetResult(string sqlQueryText)
        {
            var dataTable = new DataTable();
            using (var con = _conn())
            {
                con.Open();
                Stopwatch sw = new Stopwatch();
                sw.Start();
                using (var command = new SqlCommand(sqlQueryText, con))
                {
                    var dataReader = command.ExecuteReader();
                    dataTable.Load(dataReader);
                }
                sw.Stop();
                _logger?.Log("SQL Query elapsed={0}", sw.Elapsed);
                if (sw.Elapsed.TotalSeconds > 5)
                {
                    _logger?.LogCritical("Following generated query took a long time to run:");
                    _logger?.LogCritical(sqlQueryText);
                }
                con.Close();
            }
            return dataTable;
        }

        private string TranspileToSQL(string cypherQueryText)
        {
            var parser = new OpenCypherParser(_logger);
            var queryNode = parser.Parse(cypherQueryText);
            var plan = LogicalPlan.ProcessQueryTree(
                    parser.Parse(cypherQueryText),
                    _graphDef,
                    _logger);
            var sqlRender = new SQLRenderer(_graphDef, _logger);
            return sqlRender.RenderPlan(plan);
        }

        private DataTable RunQueryTranspiledToSQLAndGetResult(string cypherQueryText)
        {
            var sqlQueryText = TranspileToSQL(cypherQueryText);
            return RunQueryInSQLAndGetResult(sqlQueryText);
        }

        private void RunQueryAndCompare(string cypherQueryText, bool compareOrder = false)
        {
            _logger?.Log($"----------------------------------------");
            _logger?.Log($"Running cypher query:\n{cypherQueryText}");
            var resultNeo4j = RunQueryInCypherAndGetResult(cypherQueryText);
            var resultSQL = RunQueryTranspiledToSQLAndGetResult(cypherQueryText);
            var comparisonHelper = new DataTableComparisonHelper();
            comparisonHelper.CompareDataTables(resultNeo4j, resultSQL, compareOrder);
            _logger?.Log($"----------------------------------------");
        }

        [TestMethod]
        public void SanityQueryTest()
        {
            // really basic sanity test
            var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WHERE p.Name = 'Tom Hanks'
RETURN p.Name as Name, m.Title as Title
";
            RunQueryAndCompare(queryText);
        }

        [TestMethod]
        public void AdvancedPatternMatchTest()
        {
            // Check if implicit inequality was added if same type of relationships are referred
            // in a single match statement
            {
                var queryText = @"
MATCH (p:Person)-[:ACTED_IN]->(m:Movie), (p2:Person)-[:ACTED_IN]->(m:Movie)
WHERE p.Name = 'Tom Hanks'
RETURN p.Name as Name, m.Title as Title, p2.Name as CoStarName
";

                RunQueryAndCompare(queryText);
            }
        }

        [TestMethod]
        public void CaseWhenExpressionTest()
        {
            // basic test
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH p.Name as PersonName, m.Title as MovieTitle, CASE WHEN p.Name = 'Tom Hanks' THEN 1 WHEN p.Name starts with 'Tom' THEN 2 WHEN p.Name starts with 'T' THEN 3 ELSE 0 END as NameFlag
WHERE PersonName starts with 'T'
RETURN PersonName, MovieTitle,NameFlag, 
CASE WHEN PersonName = 'Tom Hanks' THEN 1 WHEN PersonName starts with 'Tom' THEN 2 WHEN PersonName starts with 'T' THEN 3 ELSE 0 END as NameFlag2
";
                RunQueryAndCompare(queryText);
            }

            // switching entities aliases 
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH p as m, m as p
MATCH (m)-[a:DIRECTED]->(p)
RETURN m.Name as Name, p.Title as Title, Case when m.Name = 'Tom Hanks' then 'This is Tom Hanks' else 'This is not Tom Hanks' end as Flag
";

                RunQueryAndCompare(queryText);
            }

            // type casting help for case when
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH p as m, m as p
MATCH (m)-[a:DIRECTED]->(p)
RETURN m.Name as Name, p.Title as Title, 
Case when m.Name = 'Tom Hanks' then 1.1 else 'This is not Tom Hanks' end as Flags
";
                RunQueryAndCompare(queryText);
            }
        }

        [TestMethod]
        public void OptionalMatchTest()
        {
            // optional match basic test
            {
                var queryText = @"
MATCH (p:Person)-[r:REVIEWED]->(m:Movie)
OPTIONAL MATCH (p)<-[:FOLLOWS]-(p2:Person)-[r2:REVIEWED]->(m)
return p.Name as Name1, p2.Name as Name2, m.Title as Title, r.Rating as Rating1, r2.Rating as Rating2
    ";
                RunQueryAndCompare(queryText);
            }
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
OPTIONAL MATCH (p)-[d:DIRECTED]->(m)
WITH p, a, m, d
WHERE p.Name = 'Tom Hanks'
RETURN p.Name as Name, m.Title as Title, count(d) > 0 as IsDirector
";
                RunQueryAndCompare(queryText);
            }

            // optional match attached to optional match
            // TODO:
            //   This query took long time to run on MS T-SQL w/ long RESOURCE_SEMAPHORE. Investigate.
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
OPTIONAL MATCH (p)-[d:DIRECTED]->(m) WHERE p.Name = 'Tom Hanks'
RETURN p.Name as Name, m.Title as Title, count(d) > 0 as IsDirector
";
                RunQueryAndCompare(queryText);
            }

            // multiple optional matches
            {
                var queryText = @"
match (p:Person) where p.Name = 'Tom Hanks' or p.Name = 'Meg Ryan'
optional match (p:Person)-[:ACTED_IN]-(m:Movie) where p.Name = 'Meg Ryan'
optional match (p:Person)-[:ACTED_IN]->(m) where p.Name = 'Tom Hanks'
return p.Name as Name, m.Title as Title
";
                RunQueryAndCompare(queryText);
            }

            // multiple optional matches (Var 2)
            {
                var queryText = @"
match (p:Person) where p.Name = 'Tom Hanks' or p.Name = 'Meg Ryan'
optional match (p:Person)-[:ACTED_IN]-(m1:Movie) where p.Name = 'Meg Ryan'
optional match (p:Person)-[:ACTED_IN]->(m2:Movie) where p.Name = 'Tom Hanks'
return p.Name as Name, m1.Title as Title1, m2.Title as Title2
";
                RunQueryAndCompare(queryText);
            }
        }

        [TestMethod]
        public void DistinctSanityTest()
        {
            // distinct test
            {
                // distinct fields
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
RETURN DISTINCT p.Name as Name
";

                RunQueryAndCompare(queryText);
            }

            {
                // distinct entity
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH DISTINCT p
RETURN p.Name as Name
";

                RunQueryAndCompare(queryText);
            }
        }

        [TestMethod]
        public void DistinctAdvancedTest()
        {
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
RETURN p.Name as Name, Count(Distinct m.Title) as TitleCount
ORDER BY Name
LIMIT 20
";
                RunQueryAndCompare(queryText);
            }

            {
                // distinct properties under "WITH"
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH DISTINCT m.Title as Title, p.Name as Name
ORDER BY Title ASC, Name DESC
LIMIT 20
WHERE Title <> 'A'
RETURN  Title, Name
    ";

                RunQueryAndCompare(queryText);
            }

            { 
                // distinct properties under "RETURN"
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH m.Title as Title, p.Name as Name 
WHERE Title <> 'A'
RETURN DISTINCT Title, Name
ORDER BY Title, Name DESC
LIMIT 20
    ";
                RunQueryAndCompare(queryText);
            }

            {
                // negative test
                // match where/order by clause field not listed in the data source
                var expectedExceptionThrown = false;
                try
                {
                    var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH DISTINCT m.Title as Title, p.Name as Name
WHERE a.Title <> 'A'
RETURN Title, Name
    ";
                    TranspileToSQL(queryText);
                }
                catch (TranspilerSyntaxErrorException e)
                {
                    Assert.IsTrue(e.Message.Contains("'a' which does not exist"));
                    expectedExceptionThrown = true;
                }
                Assert.IsTrue(expectedExceptionThrown);
            }

            {
                // negative test
                // match where/order by clause field not listed in the data source
                var expectedExceptionThrown = false;
                try
                {
                    var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH DISTINCT m.Title as Title, p.Name as Name
WHERE TitleNotExist <> 'A'
RETURN  Title, Name
    ";
                    TranspileToSQL(queryText);
                }
                catch (TranspilerSyntaxErrorException e)
                {
                    Assert.IsTrue(e.Message.Contains("'TitleNotExist' does not exist"));
                    expectedExceptionThrown = true;
                }
                Assert.IsTrue(expectedExceptionThrown);
            }
        }

        [TestMethod]
        public void OperatorTest()
        {
            // Test various operators
            // +, -, *, /, ^, IN
            {
                var queryText = @"
MATCH (p:Person)-[:ACTED_IN]-(m:Movie)
WHERE p.Name in ['Tom Hanks', 'Meg Ryan'] and m.Released >= 1990
RETURN p.Name as Name, m.Title AS Title, m.Released % 100 as ReleasedYear2Digit, m.Released - 2000 as YearSinceY2k, m.Released * 1 / 1 ^ 1 AS TestReleasedYear
";

                RunQueryAndCompare(queryText);
            }
        }

        [TestMethod]
        public void StringFunctionTest()
        {
            // Basic string function test
            // left
            // right
            // ltrim
            // rtrim
            // trim
            // toLower
            // toUpper
            // contains
            // size
            {
                var queryText = @"
MATCH (p:Person)
WHERE p.Name contains 'Tom' OR left(p.Name, 3) = 'Meg' OR right(p.Name, 3) = 'nks'
RETURN p.Name as Name, toLower(p.Name) as LName, toUpper(p.Name) as UName, size(p.Name) as NameSize, 
        trim(' ' + p.Name + ' ') AS TrimmedName,
        ltrim(' ' + p.Name + ' ') AS LTrimmedName,
        rtrim(' ' + p.Name + ' ') AS RTrimmedName
";

                RunQueryAndCompare(queryText);
            }
        }

        [TestMethod]
        public void AggregationFunctionTest()
        {
            // Currently supported
            // avg
            // sum
            // max
            // min
            // count

            // test basic aggregates
            {
                var queryText = @"
MATCH (p:Person)-[r:REVIEWED]->(m:Movie)
return m.Title as Title, AVG(r.Rating) as AvgRating, MAX(r.Rating) as HighestRating, MIN(r.Rating) as LowestRating, COUNT(r.Rating) as NumberOfReviews, SUM(r.Rating) as TotalRating, COUNT(r.Rating) / SUM(r.Rating) as AvgRating2
";

                RunQueryAndCompare(queryText);
            }

            // test count(distinct())
            {
                var queryText = @"
MATCH (p:Person)-[r:REVIEWED]->(m:Movie)
return COUNT(r.Rating) as NumberOfRatings, COUNT(DISTINCT(r.Rating)) as NumberOfUniqueRatings, COUNT(DISTINCT(m)) as NumberOfUniqueMovies
";

                RunQueryAndCompare(queryText);
            }

            // test stdev/stdevp aggregates
            // NOTE: neo4j returns 0 for STDEV for when popsize = 1, where SQL (correctly) return Null
            //       so for now, we skip comparing STDEV for this special case
            {
                var queryText = @"
MATCH (p:Person)-[r:REVIEWED]->(m:Movie)
WITH m.Title as Title, STDEV(r.Rating) as RatingStdDev, STDEVP(r.Rating) as RatingStdDevP, COUNT(DISTINCT(p)) AS PopSize
WHERE PopSize > 1
RETURN Title, RatingStdDev, RatingStdDevP, PopSize
";

                RunQueryAndCompare(queryText);
            }


            // negative test case: nested aggregations
            {
                var queryText = @"
MATCH (p:Person)-[r:REVIEWED]->(m:Movie)
return COUNT(MAX(r.Rating)) as CountOfMaxRatings
";
                try
                {
                    TranspileToSQL(queryText);
                    Assert.Fail("Negative test case passed unexpectedly.");
                }
                catch (TranspilerNotSupportedException)
                { }
            }

            // TODO: following is not supported/tested yet
            // - percentileCount
            // - percentileDisc
        }

        [TestMethod]
        public void EdgeDirectionTest()
        {
            // forward
            {
                var queryText = @"
MATCH (p:Person)-[:FOLLOWS]->(p2:Person)
return p.Name as Name1, p2.Name as Name2
    ";
                RunQueryAndCompare(queryText);
            }

            // backward
            {
                var queryText = @"
MATCH (p:Person)<-[:FOLLOWS]-(p2:Person)
return p.Name as Name1, p2.Name as Name2
    ";
                RunQueryAndCompare(queryText);
            }

            // either direction (automatically inferred)
            {
                var queryText = @"
MATCH (p:Person)-[:DIRECTED]-(m:Movie)
return p.Name as Name, m.Title as Title
    ";
                RunQueryAndCompare(queryText);
            }

            // negative test case (wrong direction)
            {
                var queryText = @"
MATCH (p:Person)<-[:DIRECTED]-(m:Movie)
return p.Name as Name, m.Title as Title
    ";
                try
                {
                    TranspileToSQL(queryText);
                    Assert.Fail("Negative test case passed unexpectedly.");
                }
                catch (TranspilerBindingException)
                { }
            }

            // negative test case of both direction with same type of src/sink
            {
                // right now we block the case of ambiguity edges (while neo4j support it right now
                // by union the match from either direction)
                var queryText = @"
MATCH (p:Person)-[:FOLLOWS]-(p2:Person)
return p.Name as Name1, p2.Name as Name2
    ";
                try
                {
                    TranspileToSQL(queryText);
                    Assert.Fail("Negative test case passed unexpectedly.");
                }
                catch (TranspilerNotSupportedException)
                { }
            }
        }

        [TestMethod]
        public void OrderByLimitClauseTest()
        {
            // basic case 1
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WITH  p.Name as Name, m.Title as Title
ORDER BY Name
LIMIT 11
RETURN Name
";
                RunQueryAndCompare(queryText);
            }

            // basic case 2
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)  
RETURN p.Name as Name, m.Title as Title
ORDER BY Name
LIMIT 11
";
                RunQueryAndCompare(queryText);
            }

            // order by nested under WITH statement
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WHERE p.Name = 'Tom Hanks'
WITH p.Name+""B"" AS Name2, m.Title AS Title
ORDER BY m.Tagline, Name2, m.Title ASC
LIMIT 10
Where p.Name starts with 'T' and Name2 starts with 'T'
RETURN Name2, Title
";

                RunQueryAndCompare(queryText);
            }

            // order by nested under return statement
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WHERE p.Name = 'Tom Hanks'
WITH p.Name AS Name2, m.Title AS Title
RETURN Name2, Title
ORDER BY Name2
LIMIT 20
";
                RunQueryAndCompare(queryText);
            }

            // order by nested under return statement
            {
                var queryText = @"
MATCH (p:Person)-[a:ACTED_IN]->(m:Movie)
WHERE p.Name = 'Tom Hanks'
RETURN p.Name as Name2 ,m.Title as Title
ORDER BY Name2
LIMIT 20
";
                RunQueryAndCompare(queryText);
            }

        }
    }
}
