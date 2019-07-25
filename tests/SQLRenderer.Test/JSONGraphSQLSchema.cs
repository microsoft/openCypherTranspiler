/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using openCypherTranspiler.Common.GraphSchema;
using openCypherTranspiler.CommonTest;
using Newtonsoft.Json;

namespace openCypherTranspiler.SQLRenderer.Test
{
    class JSONGraphSQLSchema : ISQLDBSchemaProvider
    {
        private JSONGraphSchema _schemaHelper;
        private IDictionary<string, SQLTableDescriptor> _tableDescs;

        public JSONGraphSQLSchema(string jsonDefFile)
        {
            _schemaHelper = new JSONGraphSchema(jsonDefFile);
            LoadFromFile(jsonDefFile);
        }

        private (string Verb, string NodeFrom, string NodeTo) ParseEntityId(string entityId)
        {
            var splits = entityId.Split('@');
            return (Verb: splits[1], NodeFrom: splits[0], NodeTo: splits[2]);
        }

        private void LoadFromFile(string jsonDefFile)
        {
            var res = JsonConvert.DeserializeObject<dynamic>(File.ReadAllText(jsonDefFile));
            var allnodes = ((IEnumerable<dynamic>)res.Nodes).Select(n => (string)n.Id.ToString());

            var tableDescs = ((IEnumerable<dynamic>)res.TableDescriptors).Select(d =>
                new SQLTableDescriptor()
                {
                    EntityId = d.EntityId.ToString(),
                    TableOrViewName = d.TableOrViewName.ToString()
                });

            _tableDescs = tableDescs.ToDictionary(kv => kv.EntityId, kv => kv);
        }

        public EdgeSchema GetEdgeDefinition(string edgeVerb, string fromNodeName, string toNodeName)
        {
            return _schemaHelper.GetEdgeDefinition(edgeVerb, fromNodeName, toNodeName);
        }

        public NodeSchema GetNodeDefinition(string nodeName)
        {
            return _schemaHelper.GetNodeDefinition(nodeName);
        }

        public SQLTableDescriptor GetSQLTableDescriptors(string entityId)
        {
            return _tableDescs[entityId];
        }
    }
}
