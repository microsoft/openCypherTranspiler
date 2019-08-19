using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using openCypherTranspiler.Common.GraphSchema;
using openCypherTranspiler.LogicalPlanner;
using openCypherTranspiler.openCypherParser;
using openCypherTranspiler.SQLRenderer;

namespace Simple
{
    public static class StringHelper
    {
        public static string StripMargin(this string s)
        {
            return Regex.Replace(s, @"[ \t]+\|", string.Empty);
        }
    }

    class Program
    {
        class SimpleProvider : ISQLDBSchemaProvider
        {
            private static readonly IDictionary<string, NodeSchema> Nodes = new List<NodeSchema>()
            {
                new NodeSchema()
                {
                    Name = "device",
                    NodeIdProperty = new EntityProperty()
                    {
                        PropertyName = "id",
                        DataType = typeof(string),
                        PropertyType = EntityProperty.PropertyDefinitionType.NodeJoinKey
                    },
                    Properties = Enumerable.Empty<EntityProperty>().ToList()
                },
                new NodeSchema()
                {
                    Name = "tenant",
                    NodeIdProperty = new EntityProperty()
                    {
                        PropertyName = "id",
                        DataType = typeof(string),
                        PropertyType = EntityProperty.PropertyDefinitionType.NodeJoinKey
                    },
                    Properties = Enumerable.Empty<EntityProperty>().ToList()
                },
                new NodeSchema()
                {
                    Name = "app",
                    NodeIdProperty = new EntityProperty()
                    {
                        PropertyName = "id",
                        DataType = typeof(string),
                        PropertyType = EntityProperty.PropertyDefinitionType.NodeJoinKey
                    },
                    Properties = new List<EntityProperty>()
                    {
                        new EntityProperty()
                        {
                            PropertyName = "AppName",
                            DataType = typeof(string),
                            PropertyType = EntityProperty.PropertyDefinitionType.RegularProperty
                        },
                    }
                }
            }.ToDictionary(kv => kv.Name, kv => kv);

            private static readonly IDictionary<string, EdgeSchema> Edges = new List<EdgeSchema>()
            {
                new EdgeSchema()
                {
                    Name = "belongsTo",
                    SourceNodeId = "device",
                    SourceIdProperty = new EntityProperty()
                    {
                        PropertyName = "srcid",
                        DataType = typeof(string),
                        PropertyType = EntityProperty.PropertyDefinitionType.SourceNodeJoinKey
                    },
                    SinkNodeId = "tenant",
                    SinkIdProperty = new EntityProperty()
                    {
                        PropertyName = "destid",
                        DataType = typeof(string),
                        PropertyType = EntityProperty.PropertyDefinitionType.SinkNodeJoinKey
                    },
                    Properties = Enumerable.Empty<EntityProperty>().ToList()
                },
                new EdgeSchema()
                {
                    Name = "runs",
                    SourceNodeId = "device",
                    SourceIdProperty = new EntityProperty()
                    {
                        PropertyName = "srcid",
                        DataType = typeof(string),
                        PropertyType = EntityProperty.PropertyDefinitionType.SourceNodeJoinKey
                    },
                    SinkNodeId = "app",
                    SinkIdProperty = new EntityProperty()
                    {
                        PropertyName = "destid",
                        DataType = typeof(string),
                        PropertyType = EntityProperty.PropertyDefinitionType.SinkNodeJoinKey
                    },
                    Properties = Enumerable.Empty<EntityProperty>().ToList()
                }
            }.ToDictionary(kv => $"{kv.SourceNodeId}@{kv.Name}@{kv.SinkNodeId}", kv => kv);

            private static readonly IDictionary<string, SQLTableDescriptor> Tables = new List<SQLTableDescriptor>()
            {
                new SQLTableDescriptor()
                {
                    EntityId = "device",
                    TableOrViewName = "tblDevice"
                },
                new SQLTableDescriptor()
                {
                    EntityId = "tenant",
                    TableOrViewName = "tblTenant"
                },
                new SQLTableDescriptor()
                {
                    EntityId = "app",
                    TableOrViewName = "tblApp"
                },
                new SQLTableDescriptor()
                {
                    EntityId = "device@belongsTo@tenant",
                    TableOrViewName = "tblBelongsTo"
                },
                new SQLTableDescriptor()
                {
                    EntityId = "device@runs@app",
                    TableOrViewName = "tblRuns"
                },
            }.ToDictionary(kv => kv.EntityId, kv => kv);

            public EdgeSchema GetEdgeDefinition(string edgeVerb, string fromNodeName, string toNodeName)
            {
                return Edges[$"{fromNodeName}@{edgeVerb}@{toNodeName}"];
            }

            public NodeSchema GetNodeDefinition(string nodeName)
            {
                return Nodes[nodeName];
            }

            public SQLTableDescriptor GetSQLTableDescriptors(string entityId)
            {
                // In this function, you will need to map SQL table or view to
                // the graph entities (nodes and edges). The table or the view
                // should already be normalized against the node key or
                // the edge source/sink key
                return Tables[entityId];
            }
        }

        static void Main(string[] args)
        {
            // To run this program directly after build, in shell, type:
            //   dotnet run bin/Debug/netcoreapp2.1/simple.dll

            var cypherQueryText = @"
                |MATCH (d:device)-[:belongsTo]->(t:tenant)
                |MATCH (d)-[:runs]->(a:app)
                |RETURN t.id as TenantId, a.AppName as AppName, COUNT(d) as DeviceCount
            ".StripMargin();

            Console.WriteLine("Input openCypher Query:");
            Console.WriteLine(cypherQueryText);

            var graphDef = new SimpleProvider();

            var plan = LogicalPlan.ProcessQueryTree(OpenCypherParser.Parse(cypherQueryText, null), graphDef, null);
            var sqlRender = new SQLRenderer(graphDef, null);
            var tSqlQuery = sqlRender.RenderPlan(plan);

            Console.WriteLine("Output T-SQL Query:");
            Console.WriteLine(tSqlQuery);
        }
    }
}
