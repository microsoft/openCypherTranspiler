/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.Common.Utils;
using openCypherTranspiler.openCypherParser.Common;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// QueryExpression represents an expression appears in Graph Query language
    /// </summary>
    public abstract class QueryExpression : TreeNode
    {
        /// <summary>
        /// Traversal children (and including itself) to retrieve query expression of certain 
        /// type and return it as a list
        /// </summary>
        /// <typeparam name="T">Type of the child expression to be returned</typeparam>
        /// <returns>A list of QueryExpression of type T</returns>
        public IEnumerable<T> GetChildrenQueryExpressionType<T>() where T : QueryExpression
        {
            return GetChildrenOfType<T>();
        }

        /// <summary>
        /// Static evaluation of the returned data type of the expression
        /// </summary>
        /// <returns>C# Type equivalent of the data type</returns>
        abstract public Type EvaluateType();
    }
}
