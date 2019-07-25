/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System.Collections.Generic;

namespace openCypherTranspiler.Common.GraphSchema
{
    /// <summary>
    /// Base class to descript a graph schema entity (can be a node or edge)
    /// </summary>
    public abstract class EntitySchema
    {
        /// <summary>
        /// Unique name to the node/edge. For node it is nodename, for edge it is unique
        /// combination of node from/to plus edge verb
        /// </summary>
        public abstract string Id { get; }

        /// <summary>
        /// Name of the node or edge. In case of edge it is the 'verb' only
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// List of all properties on the node/edge (combining all parts)
        /// </summary>
        public IList<EntityProperty> Properties { get; set; }
    }
}
