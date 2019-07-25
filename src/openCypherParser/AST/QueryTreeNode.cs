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
    /// holds a query or a union of sub queires
    /// </summary>
    public abstract class QueryTreeNode : TreeNode
    {
    }

    /// <summary>
    /// represents union of leaf-node queries: multiple queries can be unioned or unioned_all together
    /// </summary>
    public class InfixQueryTreeNode : QueryTreeNode
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
        public QueryTreeNode LeftQueryTreeNode { get; set; }
        public QueryTreeNode RightQueryTreeNode { get; set; }

        public override string ToString()
        {
            return $"QueryInfix: {Operator}";
        }
    }

    /// <summary>
    /// the final part of the query that has raw or nested input, conditions and output
    /// </summary>
    public class SingleQueryTreeNode : QueryTreeNode
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { PipedData }
                    .Union(EntityPropertySelected.Cast<TreeNode>())
                    .Union(EntityPropertyOrderBy??new List<QueryExpressionOrderBy>())
                    .Union(EntityRowsLimit??new List<QueryExpressionLimit>())
                    .Where(p => p != null);
            }
        }
        #endregion Implements TreeNode

        /// <summary>
        /// The input of the query. Can be either graph data (from a match statement) or output from an intermediate query
        /// </summary>
        public PartialQueryTreeNode PipedData { get; set; }
        
        /// <summary>
        /// Expressions in the return or with
        /// </summary>
        public IList<QueryExpressionWithAlias> EntityPropertySelected { get; set; }

        /// <summary>
        /// If return is called with DISTINCT modifier
        /// </summary>
        public bool IsDistinct { get; set; }

        /// <summary>
        /// Order by items in return or in with clause
        /// </summary>
        public IList<QueryExpressionOrderBy> EntityPropertyOrderBy { get; set; }

        /// <summary>
        /// For "LIMIT" modifier, holding numbers of rows to be selected in the data
        /// </summary>
        public IList<QueryExpressionLimit> EntityRowsLimit { get; set; }
        public override string ToString()
        {
            return $"QuerySingle: Props={string.Join(",", EntityPropertySelected.Select(p => p.Alias))}; Distinct={IsDistinct} ;OrderBy={string.Join(",", EntityPropertyOrderBy?.Select(p => p.Alias))} ; Limit={string.Join(",", EntityRowsLimit?.Select(p => p.RowCount))} ";
        }
    }

    /// <summary>
    /// represents match/querypart / match/querypart-with / match/querypart-with-where / match/querypart/where-with-where
    /// </summary>
    public class PartialQueryTreeNode : TreeNode
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { PipedData, MatchData }
                .Union(ProjectedExpressions)
                .Union(new List<TreeNode>() { PostCondition})
                .Union(OrderByExpression??new List<QueryExpressionOrderBy>())
                .Union(LimitExpression??new List<QueryExpressionLimit>())
                .Where(p => p != null);
            }
        }
        #endregion Implements TreeNode

        /// <summary>
        /// represents the data flow from previous partial query in the chain
        /// </summary>
        public PartialQueryTreeNode PipedData { get; set; }

        /// <summary>
        /// represents the (additional) match statements
        /// </summary>
        public MatchDataSource MatchData { get; set; }

        /// <summary>
        /// the attributes/entities to be returned to next part of the query chain
        /// </summary>
        public IList<QueryExpressionWithAlias> ProjectedExpressions { get; set; }

        /// <summary>
        /// Indicate if this query part has no WITH (hence the ProjectedExpressions are implied to be return all aliased nodes)
        /// </summary>
        public bool IsImplicitProjection { get; set; }

        /// <summary>
        /// the attributes/entities to be returned to next part of the query chain
        /// </summary>
        public bool IsDistinct { get; set; }

        /// <summary>
        /// if the next query part can be a non-optional match or not
        /// </summary>
        public bool CanChainNonOptionalMatch { get; set; }

        /// <summary>
        /// query conditions post projection
        /// </summary>
        public QueryExpression PostCondition { get; set; }

        /// <summary>
        /// ORDER BY under WITH statement
        /// </summary>
        public IList<QueryExpressionOrderBy> OrderByExpression { get; set; }
        /// <summary>
        /// LIMIT N under WITH statement
        /// </summary>
        public IList<QueryExpressionLimit> LimitExpression { get; set; }

        public override string ToString()
        {
            return $"QueryPart: HasMatch={MatchData != null}; IsImplicitProj={IsImplicitProjection}; HasCond={PostCondition != null}; Props={string.Join(",", ProjectedExpressions?.Select(p => p.Alias))}, Distinct={IsDistinct}, HasOrderBy={OrderByExpression!= null}, HasLimit = {LimitExpression != null}";
        }
    }

}
