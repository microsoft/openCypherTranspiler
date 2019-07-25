/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.Common.Utils;
using openCypherTranspiler.openCypherParser.Common;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// represents an expression appears in Graph Query language
    /// </summary>
    public abstract class QueryExpression : TreeNode
    {
        /// <summary>
        /// Traversal helper to retrieve query expression of certain type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IEnumerable<T> GetChildrenQueryExpressionType<T>() where T : QueryExpression
        {
            return GetChildrenOfType<T>();
        }

        /// <summary>
        /// Compute the result's data type of the expression
        /// </summary>
        /// <returns></returns>
        abstract public Type EvaluateType();
    }

    /// <summary>
    ///  represents a expression with an explicit alias, e.g. a AS b
    /// </summary>
    public class QueryExpressionWithAlias : QueryExpression
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
        public string Alias { get; set; }

        public override string ToString()
        {
            return $"ExprWithAlias: {Alias}";
        }

        public override Type EvaluateType()
        {
            return InnerExpression.EvaluateType();
        }
    }

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
                    if (leftType != typeof(bool) || leftType != typeof(bool?) &&
                    rightType != typeof(bool) || rightType != typeof(bool?))
                    {
                        throw new TranspilerNotSupportedException($"Logical binary operator {Operator} operating must operate on bool types. Actual types: {leftType}, {rightType}");
                    }
                    return (anyNullable ? TypeHelper.MakeNullableIfNotAlready(typeof(bool)) : typeof(bool));

                case BinaryOperatorType.Value:
                    // For value type operator, use the value type coercion table
                    if (!CoersionTables.CoersionTableForValueType.TryGetValue((Operator.Name, leftTypeUnboxed, rightTypeUnboxed), out resultedTypeRaw))
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
                        if (!CoersionTables.CoersionTableEqualityComparison.TryGetValue((leftTypeUnboxed, rightTypeUnboxed), out resultedTypeRaw))
                        {
                            throw new TranspilerInternalErrorException($"Unexpected use of binary operator {Operator.Name} operating between types {leftTypeUnboxed} and {rightTypeUnboxed}");
                        }
                    }
                    else
                    {
                        if (!CoersionTables.CoersionTableInequalityComparison.TryGetValue((leftTypeUnboxed, rightTypeUnboxed), out resultedTypeRaw))
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

    /// <summary>
    /// represents a function call, like toFloat(expr)
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

    /// <summary>
    /// represents a aggregation function call, like avg(expr)
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
                // for any aggregation function that were not having specially handling, then it is considered to preserve the original type
                return innerType;
            }

            return resultedType;
        }
    }

    /// <summary>
    /// represents a reference to a property (column), e.g. r.Score
    /// </summary>
    public class QueryExpressionProperty : QueryExpression
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return Enumerable.Empty<TreeNode>();
            }
        }
        #endregion Implements TreeNode

        /// <summary>
        /// For a property reference, this is the variable part, namely alias of alias.field
        /// </summary>
        public string VariableName { get; set; }

        /// <summary>
        /// For a property reference, this is the property part, namely alias of alias.field
        /// </summary>
        public string PropertyName { get; set; }


        /// <summary>
        /// For a node/relationship, this is the entity type reference
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public Entity Entity { get; set; }

        /// <summary>
        /// For a single field, this is the data type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public Type DataType { get; set; }

        public override string ToString()
        {
            if (Entity != null)
            {
                return $"ExprProperty: {VariableName} {Entity}";
            }
            else
            {
                return $"ExprProperty: {VariableName}.{PropertyName}";
            }
        }
        public override Type EvaluateType()
        {
            return DataType;
        }

    }

    /// <summary>
    /// represent a list of expressions
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

    /// <summary>
    /// represents a literal value, such as a string, or number
    /// </summary>
    public class QueryExpressionValue : QueryExpression
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return Enumerable.Empty<TreeNode>();
            }
        }
        #endregion Implements TreeNode

        private object _value;

        /// <summary>
        /// Holding the object value in a supported value-type
        /// </summary>
        public object Value
        {
            get
            {
                return _value;
            }
            set
            {
                if (!(value is string ||
                      value is bool ||
                      value is int ||
                      value is long ||
                      value is float ||
                      value is double ||
                      value is DateTime))
                {
                    throw new TranspilerNotSupportedException($"Type {value.GetType()}");
                }
                _value = value;
            }
        }

        public Type ValueType
        {
            get
            {
                return Value.GetType();
            }
        }
        public bool BoolValue
        {
            get
            {
                return (Value is bool ? (bool)Value : Convert.ToBoolean(Value));
            }
            set
            {
                Value = value;
            }
        }
        public int IntValue
        {
            get
            {
                return (Value is int ? (int)Value : Convert.ToInt32(Value));
            }
            set
            {
                Value = value;
            }
        }
        public long LongValue
        {
            get
            {
                return (Value is long ? (long)Value : Convert.ToInt64(Value));
            }
            set
            {
                Value = value;
            }
        }
        public double DoubleValue
        {
            get
            {
                return (Value is double ? (double)Value : Convert.ToDouble(Value));
            }
            set
            {
                Value = value;
            }
        }
        public DateTime DateTimeValue
        {
            get
            {
                return (Value is DateTime ? (DateTime)Value : Convert.ToDateTime(Value));
            }
            set
            {
                Value = value;
            }
        }
        public string StringValue
        {
            get
            {
                return (Value is string ? (string)Value : Value.ToString());
            }
            set
            {
                Value = value;
            }
        }

        public override string ToString()
        {
            return $"ExprValue: t='{Value?.GetType()}'; v='{Value?.ToString()}'";
        }

        public override Type EvaluateType()
        {
            return ValueType;
        }
    }

    public class QueryExpressionOrderBy : QueryExpression
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
            return $"ExprOrderBy: {InnerExpression.GetChildrenQueryExpressionType<QueryExpressionProperty>().First().VariableName}";
        }

        public override Type EvaluateType()
        {
            return InnerExpression.EvaluateType();
        }

    }
    public class QueryExpressionLimit : QueryExpression
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
        /// stores how many rows to keep in "LIMIT" clause
        /// </summary>
        public int RowCount { get; set; }
                
        public override string ToString()
        {
            return $"ExprLimit: {InnerExpression.GetChildrenQueryExpressionType<QueryExpressionProperty>().First().VariableName}";
        }

        public override Type EvaluateType()
        {
            return InnerExpression.EvaluateType();
        }

    }

    public class QueryExpressionCaseExpression : QueryExpression
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { InitialCaseExpression, ElseExpression }.Union(CaseAlternatives).Where(n => n != null);
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
            return $"CaseExpr: Case when {CaseAlternatives.ToString()} Else:{ElseExpression}";
        }

        public override Type EvaluateType()
        {
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
                // During parsing, type evalution allows the max extent of type coercion
                // which is what a + operator would do
                resultUnboxedType = distinctUnboxedTypes.Aggregate((resType, nextType) =>
                {
                    Type resultedTypeRaw;
                    if (!CoersionTables.CoersionTableForValueType.TryGetValue((BinaryOperator.Plus, resType, nextType), out resultedTypeRaw))
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
            return $"ExprCaseAlter: when ->{WhenExpression.ToString()} Then -> {ThenExpression.ToString()}";
        }
   
        public override Type EvaluateType()
        {
            return ThenExpression.EvaluateType();
        }


    }


}
