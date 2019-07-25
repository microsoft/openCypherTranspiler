/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using openCypherTranspiler.Common.GraphSchema;
using System.Collections.Generic;

namespace openCypherTranspiler.SQLRenderer
{
    public interface ISQLDBSchemaProvider : IGraphSchemaProvider
    {
        SQLTableDescriptor GetSQLTableDescriptors(string entityId);
    }
}
