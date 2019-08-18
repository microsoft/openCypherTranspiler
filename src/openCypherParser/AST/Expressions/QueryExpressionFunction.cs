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
    /// Represents a function call, like toFloat(expr)
    /// </summary>
    public class QueryExpressionFunction : QueryExpression
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

        public FunctionInfo Function { get; set; }

        public QueryExpression InnerExpression { get; set; }

        public IEnumerable<QueryExpression> AdditionalExpressions { get; set; }

        public override string ToString()
        {
            return $"ExprFunc: {Function}(a)";
        }

        public override Type EvaluateType()
        {
            var innerType = InnerExpression.EvaluateType();
            var isWrappedinNullable = TypeHelper.IsSystemNullableType(innerType);
            switch (Function.FunctionName)
            {
                case Common.Function.ToFloat:
                    return isWrappedinNullable ? typeof(float?) : typeof(float);
                case Common.Function.ToString:
                    return typeof(string);
                case Common.Function.ToBoolean:
                    return isWrappedinNullable ? typeof(bool?) : typeof(bool);
                case Common.Function.ToInteger:
                    return isWrappedinNullable ? typeof(int?) : typeof(int);
                case Common.Function.ToDouble:
                    return isWrappedinNullable ? typeof(long?) : typeof(long);
                case Common.Function.ToLong:
                    return isWrappedinNullable ? typeof(double?) : typeof(double);
                case Common.Function.Not:
                    return isWrappedinNullable ? typeof(bool?) : typeof(bool);
                case Common.Function.StringContains:
                case Common.Function.StringStartsWith:
                case Common.Function.StringEndsWith:
                case Common.Function.IsNull:
                case Common.Function.IsNotNull:
                    return typeof(bool);
                case Common.Function.StringSize:
                    return typeof(int);
                default:
                    // treat all the rest as type preserving, e.g.
                    // trim, ltrim ....
                    return InnerExpression.EvaluateType();
            }
        }

    }
}
