/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Linq;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// Represent a list of expressions
    /// </summary>
    public class QueryExpressionList : QueryExpression
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return ExpressionList.Cast<TreeNode>();
            }
        }
        #endregion Implements TreeNode

        public IList<QueryExpression> ExpressionList { get; set; }

        public override Type EvaluateType()
        {
            var types = ExpressionList?.Select(p => p.EvaluateType()).Distinct();
            if (types.Count() == 1)
            {
                return typeof(IEnumerable<>).MakeGenericType(types.First());
            }
            else
            {
                return typeof(IEnumerable<object>);
            }
        }
    }
}
