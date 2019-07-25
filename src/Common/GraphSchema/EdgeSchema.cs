/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


namespace openCypherTranspiler.Common.GraphSchema
{
    public class EdgeSchema : EntitySchema
    {
        public static char Separator = '@';

        public static string GetEdgeId(string verb, string nodeFromName, string nodeToName)
        {
            return $"{nodeFromName}{Separator}{verb}{Separator}{nodeToName}";
        }

        /// <summary>
        /// The identifier of edge
        /// </summary>
        public override string Id
        {
            get
            {
                return GetEdgeId(Name, SourceNodeId, SinkNodeId);
            }
        }

        public EntityProperty SourceIdProperty { get; set; }

        public EntityProperty SinkIdProperty { get; set; }

        /// <summary>
        /// The identifier of the source node schema this edge schema links to
        /// </summary>
        public string SourceNodeId { get; set; }

        /// <summary>
        /// The identifier of the sink node schema this edge schema links to
        /// </summary>
        public string SinkNodeId { get; set; }

    }
}
