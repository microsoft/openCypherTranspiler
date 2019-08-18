/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Linq;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.Common.Utils;
using openCypherTranspiler.openCypherParser.Common;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// Represents a single branch WHEN THEN expression the CASE expression
    /// </summary>
    public class QueryExpressionCaseAlternative : QueryExpression
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { WhenExpression, ThenExpression };
            }
        }
        #endregion Implements TreeNode

        public QueryExpression WhenExpression { get; set; }
        public QueryExpression ThenExpression { get; set; }

        public override string ToString()
        {
            return $"ExprCaseAlter: When->{WhenExpression.ToString()} Then->{ThenExpression.ToString()}";
        }

        public override Type EvaluateType()
        {
            return ThenExpression.EvaluateType();
        }


    }

    /// <summary>
    /// Represents a switching expression like CASE WHEN THEN
    /// </summary>
    public class QueryExpressionCaseExpression : QueryExpression
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { InitialCaseExpression, ElseExpression }
                    .Union(CaseAlternatives).Where(n => n != null);
            }
        }
        #endregion Implements TreeNode

        // For "CASE" part in the case expression
        public QueryExpression InitialCaseExpression { get; set; }

        // For :"WHEN ... THEN" part in the case expression
        // Can be null if "WHEN ... THEN ..." not specified in the query
        public List<QueryExpressionCaseAlternative> CaseAlternatives { get; set; }

        // For"ELSE ..." part in the case expression
        public QueryExpression ElseExpression { get; set; }

        public override string ToString()
        {
            return $"CaseExpr: Case {CaseAlternatives.ToString()} Else->{ElseExpression?.ToString() ?? "(None)"}";
        }

        public override Type EvaluateType()
        {
            // CASE WHEN's type depends on the most generous type from its branches
            // E.g.  CASE WHEN <num> ELSE <string> END gives a string type
            var hasElseExpression = (ElseExpression != null);
            var distinctTypes = hasElseExpression ?
                CaseAlternatives.Select(n => n.EvaluateType())
                    .Union(new List<Type> { ElseExpression.EvaluateType() })
                    .Distinct() :
                CaseAlternatives.Select(n => n.EvaluateType())
                    .Distinct();
            var anyNullable = distinctTypes.Any(t => TypeHelper.IsSystemNullableType(t))
                // when Else statement is not provided resulting type becomes automatically nullable
                || !hasElseExpression;
            var distinctUnboxedTypes = distinctTypes
                .Select(t => TypeHelper.GetUnderlyingTypeIfNullable(t)).Distinct();

            Type resultUnboxedType;
            if (distinctUnboxedTypes.Count() > 1)
            {
                resultUnboxedType = distinctUnboxedTypes.Aggregate((resType, nextType) =>
                {
                    Type resultedTypeRaw;
                    if (!TypeCoersionTables.CoersionTableForValueType.TryGetValue((BinaryOperator.Plus, resType, nextType), out resultedTypeRaw))
                    {
                        throw new TranspilerInternalErrorException($"Unexpected use of CASE WHEN operating between types {resType} and {nextType}");
                    }
                    if (resultedTypeRaw == default(Type))
                    {
                        throw new TranspilerNotSupportedException($"Case WHEN operating between types {resType} and {nextType}");
                    }
                    return resultedTypeRaw;
                });
            }
            else
            {
                resultUnboxedType = distinctUnboxedTypes.First();
            }

            return (anyNullable ? TypeHelper.MakeNullableIfNotAlready(resultUnboxedType) : resultUnboxedType);
        }
    }
}
