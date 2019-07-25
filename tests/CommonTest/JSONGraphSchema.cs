/*---------------------------------------------------------------------------------------------
 * Copyright(c) Microsoft Corporation.All rights reserved.
 *  Licensed under the MIT License.See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using openCypherTranspiler.Common.GraphSchema;

namespace openCypherTranspiler.CommonTest
{
    public class JSONGraphSchema : IGraphSchemaProvider
    {
        private IList<NodeSchema> _allNodeDefinions;
        private IList<EdgeSchema> _allEdgeDefinions;

        public JSONGraphSchema(string file)
        {
            LoadFromFile(file);
        }

        private void LoadFromFile(string file)
        {
            var res = JsonConvert.DeserializeObject<dynamic>(File.ReadAllText(file));

            var allnodes = new Dictionary<string, NodeSchema>();
            foreach (var n in res.Nodes)
            {
                var nodeName = n.Name.ToString();
                var nodeDef = new NodeSchema()
                {
                    Name = nodeName,
                    Properties = (n.Properties?.ToObject<List<dynamic>>() as List<dynamic> ?? Enumerable.Empty<dynamic>())
                        .Select(s => (new EntityProperty()
                        {
                            PropertyName = s.PropertyName.ToString(),
                            DataType = Type.GetType(s.PropertyType.ToString()),
                            PropertyType = EntityProperty.PropertyDefinitionType.RegularProperty
                        })).ToList(),
                    NodeIdProperty = new EntityProperty()
                    {
                        PropertyName = n.IdProperty.PropertyName.ToString(),
                        DataType = Type.GetType(n.IdProperty.PropertyType.ToString()),
                        PropertyType = EntityProperty.PropertyDefinitionType.NodeJoinKey
                    },
                };
                allnodes.Add(nodeName, nodeDef);
            }
            _allNodeDefinions = allnodes.Values.ToList();

            _allEdgeDefinions = new List<EdgeSchema>();
            foreach (var e in res.Edges)
            {
                _allEdgeDefinions.Add(new EdgeSchema()
                {
                    Name = e.Name.ToString(),
                    SourceNodeId= e.FromNode.ToString(),
                    SinkNodeId = e.ToNode.ToString(),
                    Properties = (e.Properties?.ToObject<List<dynamic>>() as List<dynamic> ?? Enumerable.Empty<dynamic>())
                        .Select(s => (new EntityProperty()
                        {
                            PropertyName = s.PropertyName.ToString(),
                            DataType = Type.GetType(s.PropertyType.ToString())
                        })).ToList(),
                    SourceIdProperty = new EntityProperty()
                    {
                        PropertyName = e.SourceIdProperty.PropertyName.ToString(),
                        DataType = Type.GetType(e.SourceIdProperty.PropertyType.ToString()),
                        PropertyType = EntityProperty.PropertyDefinitionType.NodeJoinKey
                    },
                    SinkIdProperty = new EntityProperty()
                    {
                        PropertyName = e.SinkIdProperty.PropertyName.ToString(),
                        DataType = Type.GetType(e.SinkIdProperty.PropertyType.ToString()),
                        PropertyType = EntityProperty.PropertyDefinitionType.NodeJoinKey
                    },
                });
            }
        }

        public EdgeSchema GetEdgeDefinition(string edgeVerb, string fromNodeName, string toNodeName)
        {
            var edge = _allEdgeDefinions?.Where(n => n.Name == edgeVerb && n.SourceNodeId == fromNodeName && n.SinkNodeId == toNodeName).FirstOrDefault();
            if (edge == null)
            {
                throw new KeyNotFoundException($"Edge with edgeverb = {edgeVerb}, fromNodeName = {fromNodeName} and toNodename = {toNodeName} not found!");
            }
            return edge;
        }

        public NodeSchema GetNodeDefinition(string nodeName)
        {
            var node = _allNodeDefinions?.Where(n => n.Name == nodeName).FirstOrDefault();
            if (node == null)
            {
                throw new KeyNotFoundException($"Node with name = {nodeName} not found!");
            }
            return node;
        }
        
    }

}
