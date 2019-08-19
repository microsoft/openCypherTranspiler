/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Linq;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// Represent a leaf node graph query (hence no union)
    /// A leaf node query always comes with a RETURN clause
    /// </summary>
    public class SingleQueryNode : QueryNode
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { PipedData }
                    .Union(EntityPropertySelected.Cast<TreeNode>())
                    .Union(OrderByClause?.Cast<TreeNode>() ?? new List<TreeNode>())
                    .Union(Limit != null ? new List<TreeNode>() { Limit } : new List<TreeNode>())
                    .Where(p => p != null);
            }
        }
        #endregion Implements TreeNode

        /// <summary>
        /// The input of the query. Can be either graph data (from a match statement) or output from an intermediate query
        /// </summary>
        public PartialQueryNode PipedData { get; set; }
        
        /// <summary>
        /// Expressions in the return or with
        /// </summary>
        public IList<QueryExpressionWithAlias> EntityPropertySelected { get; set; }

        /// <summary>
        /// If return is called with DISTINCT modifier
        /// </summary>
        public bool IsDistinct { get; set; }

        /// <summary>
        /// A list (order matters) of sort items in an OrderBy clause
        /// </summary>
        public IList<SortItem> OrderByClause { get; set; }

        /// <summary>
        /// For "LIMIT" modifier, holding numbers of rows to be selected in the data
        /// </summary>
        public LimitClause Limit { get; set; }

        public override string ToString()
        {
            return $"QuerySingle: Props={string.Join(",", EntityPropertySelected.Select(p => p.Alias))}; Distinct={IsDistinct}; OrderBy={string.Join(",", OrderByClause?.Select(p => p.Alias) ?? Enumerable.Empty<string>())}; Limit={Limit?.RowCount}";
        }
    }
}
