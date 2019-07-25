/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System;

namespace openCypherTranspiler.Common.GraphSchema
{
    public class EntityProperty
    {
        public enum PropertyDefinitionType
        {
            NodeJoinKey,
            SourceNodeJoinKey,
            SinkNodeJoinKey,
            RegularProperty,
            Label,
            SourceNodeLabel,
            SinkNodeLabel,
            NodeSampleKey,
            SourceNodeSampleKey,
            SinkNodeSampleKey
        };

        public string PropertyName { get; set; }
        public Type DataType { get; set; }
        public PropertyDefinitionType PropertyType { get; set; }

    }
}
