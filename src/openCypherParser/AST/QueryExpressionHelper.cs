/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using openCypherTranspiler.openCypherParser.AST;

namespace openCypherTranspiler.openCypherParser.AST
{
    public static class QueryExpressionHelper
    {
        /// <summary>
        /// If expr is a direct return of an entity, return it, otherwise, return null
        /// </summary>
        /// <param name="expr"></param>
        /// <returns></returns>
        public static Entity TryGetDirectlyExposedEntity(this QueryExpression expr)
        {
            if (expr is QueryExpressionProperty)
            {
                var queryEntityExpr = expr as QueryExpressionProperty;
                var entity = queryEntityExpr.Entity;
                return entity;
            }
            return null;
        }
    }
}
