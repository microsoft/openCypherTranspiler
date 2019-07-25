/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


namespace openCypherTranspiler.Common.GraphSchema
{
    public class NodeSchema : EntitySchema
    {
        public override string Id
        {
            get
            {
                return Name;
            }
        }
        public EntityProperty NodeIdProperty { get; set; }
    }
}

