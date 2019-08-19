/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using openCypherTranspiler.Common.Utils;
using openCypherTranspiler.openCypherParser.Common;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// Represents an aggregation function, like avg(expr)
    /// </summary>
    public partial class QueryExpressionAggregationFunction : QueryExpression
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

        public AggregationFunction AggregationFunction { get; set; }
        public bool IsDistinct { get; set; }
        public QueryExpression InnerExpression { get; set; }

        public override string ToString()
        {
            return $"ExprAggFunc: {AggregationFunction}(a)";
        }

        public override Type EvaluateType()
        {
            var innerType = InnerExpression.EvaluateType();
            var innerTypeUnboxed = TypeHelper.GetUnderlyingTypeIfNullable(innerType);

            Type resultedType;
            if (!AggregationFunctionReturnTypeTable.TypeMapTable.TryGetValue((AggregationFunction, innerTypeUnboxed), out resultedType))
            {
                // for any aggregation function that were not having specially handling, we treat it as original type preserving
                return innerType;
            }

            return resultedType;
        }
    }
}
