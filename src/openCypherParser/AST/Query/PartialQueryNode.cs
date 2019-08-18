/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// Represents component of a single query that returns graph data
    /// A query part can contain MATCH, WHERE, WITH clauses
    /// </summary>
    public class PartialQueryNode : TreeNode
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { PipedData, MatchData }
                .Union(ProjectedExpressions)
                .Union(new List<TreeNode>() { FilterExpression})
                .Union(OrderByClause?.Cast<TreeNode>() ?? new List<TreeNode>())
                .Union(Limit != null ? new List<TreeNode>() { Limit } : new List<TreeNode>())
                .Where(p => p != null);
            }
        }
        #endregion Implements TreeNode

        /// <summary>
        /// represents the data flow from previous partial query in the chain
        /// </summary>
        public PartialQueryNode PipedData { get; set; }

        /// <summary>
        /// represents the (additional) match statements
        /// </summary>
        public MatchClause MatchData { get; set; }

        /// <summary>
        /// the attributes/entities to be returned to next part of the query chain
        /// </summary>
        public IList<QueryExpressionWithAlias> ProjectedExpressions { get; set; }

        /// <summary>
        /// Indicate if this query part has no WITH (hence the ProjectedExpressions are implied to be return all aliased nodes)
        /// </summary>
        public bool IsImplicitProjection { get; set; }

        /// <summary>
        /// If DISTINCT was specified in WITH clause or not
        /// </summary>
        public bool IsDistinct { get; set; }

        /// <summary>
        /// If the next query part can be a non-optional match or not
        /// </summary>
        public bool CanChainNonOptionalMatch { get; set; }

        /// <summary>
        /// Root expression from the expression specified WHERE clause
        /// </summary>
        public QueryExpression FilterExpression { get; set; }

        /// <summary>
        /// ORDER BY clause (optional)
        /// </summary>
        public IList<SortItem> OrderByClause { get; set; }

        /// <summary>
        /// LIMIT N clause (optional)
        /// </summary>
        public LimitClause Limit { get; set; }

        public override string ToString()
        {
            return $"QueryPart: HasMatch={MatchData != null}; IsImplicitProj={IsImplicitProjection}; HasCond={FilterExpression != null}; Props={string.Join(",", ProjectedExpressions?.Select(p => p.Alias))}, Distinct={IsDistinct}, HasOrderBy={OrderByClause!= null}, HasLimit={Limit != null}";
        }
    }

}
