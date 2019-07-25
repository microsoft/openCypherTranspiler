/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System.Collections.Generic;
using System.Threading.Tasks;

namespace openCypherTranspiler.Common.GraphSchema
{
    /// <summary>
    /// Implements by the Graph Schema provider to allow query the graph definition
    /// </summary>
    public interface IGraphSchemaProvider
    {
        /// <summary>
        /// Return a NodeDefinition for the given node name.
        /// </summary>
        /// <param name="nodeName"></param>
        /// <exception cref="KeyNotFoundException">if node name was not found</exception>
        /// <returns></returns>
        NodeSchema GetNodeDefinition(string nodeName);

        /// <summary>
        /// Return a NodeDefinition for the given edge verb, source and sink entity name.
        /// </summary>
        /// <param name="edgeVerb"></param>
        /// <param name="fromNodeName"></param>
        /// <param name="toNodeName"></param>
        /// <exception cref="KeyNotFoundException">if node name was not found</exception>
        /// <returns></returns>
        EdgeSchema GetEdgeDefinition(string edgeVerb, string fromNodeName, string toNodeName);
    }
}
