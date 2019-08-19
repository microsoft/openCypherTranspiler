/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// Represents union of two sub-queries
    /// </summary>
    public class InfixQueryNode : QueryNode
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { LeftQueryTreeNode, RightQueryTreeNode };
            }
        }
        #endregion Implements TreeNode

        public enum QueryOperator
        {
            Union, // dup removal
            UnionAll, // no dup removal
        }

        public QueryOperator Operator { get; set; }
        public QueryNode LeftQueryTreeNode { get; set; }
        public QueryNode RightQueryTreeNode { get; set; }

        public override string ToString()
        {
            return $"QueryInfix: {Operator}";
        }
    }
}
