/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
 
using System;
using System.Collections.Generic;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// Represents an expression with an assigned alias, e.g. a AS b
    /// </summary>
    public class QueryExpressionWithAlias : QueryExpression
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { InnerExpression };
            }
        }
        #endregion Implements TreeNode

        public QueryExpression InnerExpression { get; set; }
        public string Alias { get; set; }

        public override string ToString()
        {
            return $"ExprWithAlias: {Alias}";
        }

        public override Type EvaluateType()
        {
            return InnerExpression.EvaluateType();
        }
    }
}
