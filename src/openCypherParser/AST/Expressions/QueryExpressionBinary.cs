/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.Common.Utils;
using openCypherTranspiler.openCypherParser.Common;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// represents a binary (a + b, or a = b, or a <= b) operation
    /// </summary>
    public partial class QueryExpressionBinary : QueryExpression
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { LeftExpression, RightExpression };
            }
        }
        #endregion Implements TreeNode

        public BinaryOperatorInfo Operator { get; set; }
        public QueryExpression LeftExpression { get; set; }
        public QueryExpression RightExpression { get; set; }

        public override string ToString()
        {
            return $"ExprBinary: Op='{Operator}'";
        }

        public override Type EvaluateType()
        {
            var leftType = LeftExpression.EvaluateType();
            var rightType = RightExpression.EvaluateType();
            var leftTypeUnboxed = TypeHelper.GetUnderlyingTypeIfNullable(leftType);
            var rightTypeUnboxed = TypeHelper.GetUnderlyingTypeIfNullable(rightType);
            var anyNullable = TypeHelper.IsSystemNullableType(leftType) || TypeHelper.IsSystemNullableType(rightType);
            Type resultedTypeRaw;

            switch (Operator.Type)
            {
                case BinaryOperatorType.Logical:
                    // For logical comparison, we ensure that all operands' type are logical already
                    // The return type is always boolean (logical)
                    if ((leftType != typeof(bool) && leftType != typeof(bool?)) ||
                        (rightType != typeof(bool) && rightType != typeof(bool?)) )
                    {
                        throw new TranspilerNotSupportedException($"Logical binary operator {Operator} operating must operate on bool types. Actual types: {leftType}, {rightType}");
                    }
                    return (anyNullable ? TypeHelper.MakeNullableIfNotAlready(typeof(bool)) : typeof(bool));

                case BinaryOperatorType.Value:
                    // For value type operator, use the value type coercion table
                    if (!TypeCoersionTables.CoersionTableForValueType.TryGetValue((Operator.Name, leftTypeUnboxed, rightTypeUnboxed), out resultedTypeRaw))
                    {
                        throw new TranspilerInternalErrorException($"Unexpected use of binary operator {Operator.Name} operating between types {leftTypeUnboxed} and {rightTypeUnboxed}");
                    }
                    if (resultedTypeRaw == default(Type))
                    {
                        throw new TranspilerNotSupportedException($"Binary operator {Operator.Name} operating between types {leftTypeUnboxed} and {rightTypeUnboxed}");
                    }
                    return (anyNullable ? TypeHelper.MakeNullableIfNotAlready(resultedTypeRaw) : resultedTypeRaw);

                case BinaryOperatorType.Comparison:
                    // For comparison operators, use the equality/inequality type coercion table
                    if (Operator.Name == BinaryOperator.EQ || Operator.Name == BinaryOperator.NEQ)
                    {
                        if (!TypeCoersionTables.CoersionTableEqualityComparison.TryGetValue((leftTypeUnboxed, rightTypeUnboxed), out resultedTypeRaw))
                        {
                            throw new TranspilerInternalErrorException($"Unexpected use of (un)equality operator {Operator.Name} operating between types {leftTypeUnboxed} and {rightTypeUnboxed}");
                        }
                    }
                    else if (Operator.Name == BinaryOperator.IN)
                    {
                        // IN need special handling as right operand is a list
                        var innerTypes = rightTypeUnboxed.GetGenericArguments();
                        if (innerTypes == null || innerTypes.Length != 1)
                        {
                            throw new TranspilerInternalErrorException($"Unexpected use of IN operator, the right type {rightTypeUnboxed} is not a list of value type");
                        }
                        resultedTypeRaw = typeof(bool);
                    }
                    else
                    {
                        if (!TypeCoersionTables.CoersionTableDefaultComparison.TryGetValue((leftTypeUnboxed, rightTypeUnboxed), out resultedTypeRaw))
                        {
                            throw new TranspilerInternalErrorException($"Unexpected use of binary operator {Operator.Name} operating between types {leftTypeUnboxed} and {rightTypeUnboxed}");
                        }
                    }
                    if (resultedTypeRaw == default(Type))
                    {
                        throw new TranspilerNotSupportedException($"Binary operator {Operator.Name} operating between types {leftTypeUnboxed} and {rightTypeUnboxed}");
                    }
                    return (anyNullable ? TypeHelper.MakeNullableIfNotAlready(resultedTypeRaw) : resultedTypeRaw);

                case BinaryOperatorType.Invalid:
                default:
                    throw new TranspilerInternalErrorException($"Unexpected operator type: {Operator.Type}");
            }
        }

    }
}
