/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
 
using System.Collections.Generic;
using System.Linq;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// Represent a sorting field plus direction in Order By clause
    /// </summary>
    public class SortItem : TreeNode
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

        /// <summary>
        /// indicator for sorting orders
        /// </summary>
        public bool IsDescending { get; set; }

        /// <summary>
        /// sorting keys
        /// </summary>
        public string Alias { get; set; }

        public override string ToString()
        {
            return $"SortItem: {InnerExpression.GetChildrenQueryExpressionType<QueryExpressionProperty>().FirstOrDefault()?.VariableName}";
        }
    }
}
