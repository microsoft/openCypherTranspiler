/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.Common.Logging;
using openCypherTranspiler.Common.Utils;
using openCypherTranspiler.LogicalPlanner;
using openCypherTranspiler.openCypherParser.AST;
using openCypherTranspiler.openCypherParser.Common;

namespace openCypherTranspiler.SQLRenderer
{
    public class SQLRenderer
    {
        // Cached graph definition object
        private readonly ISQLDBSchemaProvider _graphDef;

        // Cached logger
        private readonly ILoggable _logger;

        // Map from operator type to the pattern need to used to render valid SQL
        private static readonly IDictionary<BinaryOperator, string> OperatorRenderPattern = new Dictionary<BinaryOperator, string>()
            {
                { BinaryOperator.Plus, "({0})+({1})" },
                { BinaryOperator.Minus, "({0})-({1})" },
                { BinaryOperator.Multiply, "({0})*({1})" },
                { BinaryOperator.Divide, "({0})/({1})" },
                { BinaryOperator.Modulo, "({0})%({1})" },
                { BinaryOperator.Exponentiation, "CAST(POWER({0},{1}) AS float)" },

                { BinaryOperator.AND, "({0}) AND ({1})" },
                { BinaryOperator.OR, "({0}) OR ({1})" },
                { BinaryOperator.XOR, "(({0}) AND NOT ({1})) OR (NOT ({0}) AND ({1}))" },

                { BinaryOperator.LT, "({0})<({1})" },
                { BinaryOperator.LEQ, "({0})<=({1})" },
                { BinaryOperator.GT, "({0})>({1})" },
                { BinaryOperator.GEQ, "({0})>=({1})" },
                { BinaryOperator.EQ, "({0})=({1})" },
                { BinaryOperator.NEQ, "({0})!=({1})" },
                { BinaryOperator.REGMATCH, "PATINDEX('%{1}%', {0})" },
                { BinaryOperator.IN, "({0}) IN {1}" },
            };

        // Map from operator type for CAST statement when need arises in some situlations such as CASE WHEN
        private static readonly IDictionary<Type, SqlDbType> TypeToSQLTypeMapping = new Dictionary<Type, SqlDbType>()
            {
                // https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-data-type-mappings
                { typeof(int), SqlDbType.Int},
                { typeof(short), SqlDbType.SmallInt},
                { typeof(long), SqlDbType.BigInt},
                { typeof(double), SqlDbType.Float},
                { typeof(string), SqlDbType.NVarChar},
                { typeof(float), SqlDbType.Float},
                { typeof(DateTime), SqlDbType.DateTime2}, // https://docs.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql?view=sql-server-2017
                { typeof(bool), SqlDbType.Bit},
                { typeof(Guid), SqlDbType.UniqueIdentifier},
                { typeof(uint), SqlDbType.Int},
                { typeof(ushort), SqlDbType.SmallInt},
                { typeof(ulong), SqlDbType.BigInt},
                { typeof(byte), SqlDbType.TinyInt},
                { typeof(byte[]), SqlDbType.Binary},
                { typeof(decimal), SqlDbType.Decimal},
            };

        private static readonly IDictionary<SqlDbType, string> SQLTypeRenderingMapping = new Dictionary<SqlDbType, string>()
            {
                // Doc: https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-data-type-mappings
                { SqlDbType.Int, "int" },
                { SqlDbType.SmallInt, "smallint" },
                { SqlDbType.BigInt, "bigint" },
                { SqlDbType.Float, "float" },
                { SqlDbType.NVarChar, "nvarchar(MAX)" },
                { SqlDbType.DateTime2, "datetime2" },
                { SqlDbType.Bit, "bit"},
                { SqlDbType.UniqueIdentifier, "uniqueidentifier" },
                { SqlDbType.TinyInt, "tinyint" },
                { SqlDbType.Binary, "binary" },
                { SqlDbType.Decimal, "decimal" },
            };

        // Map from Aggregation Function to its equivalent in SQL
        private static readonly IDictionary<AggregationFunction, string> AggregationFunctionRenderPattern = new Dictionary<AggregationFunction, string>()
            {
                { AggregationFunction.Avg, "AVG(CAST({0} AS float))"},
                { AggregationFunction.Sum, "SUM({0})"},
                { AggregationFunction.Min, "MIN({0})"},
                { AggregationFunction.Max, "MAX({0})"},
                { AggregationFunction.First, "MIN({0})"},
                { AggregationFunction.Last, "MAX({0})"},
                { AggregationFunction.StDev, "STDEV({0})" },
                { AggregationFunction.StDevP, "STDEVP({0})" },
            };

        enum ConversionType
        {
            NoNeed,
            Convert,
            Cast,
            Invalid
        }

        private static readonly IDictionary<(SqlDbType, SqlDbType), ConversionType> SQLTypeConversionType = new Dictionary<(SqlDbType, SqlDbType), ConversionType>()
            {
                // Doc: https://docs.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-2017
                { (SqlDbType.Int, SqlDbType.Int), ConversionType.NoNeed },
                { (SqlDbType.Int, SqlDbType.SmallInt), ConversionType.Cast },
                { (SqlDbType.Int, SqlDbType.BigInt), ConversionType.Cast },
                { (SqlDbType.Int, SqlDbType.Float), ConversionType.Cast },
                { (SqlDbType.Int, SqlDbType.NVarChar), ConversionType.Cast },
                { (SqlDbType.Int, SqlDbType.DateTime2), ConversionType.Invalid },
                { (SqlDbType.Int, SqlDbType.Bit), ConversionType.Cast },
                { (SqlDbType.Int, SqlDbType.UniqueIdentifier), ConversionType.Invalid },
                { (SqlDbType.Int, SqlDbType.TinyInt), ConversionType.Cast },
                { (SqlDbType.Int, SqlDbType.Binary), ConversionType.Cast },
                { (SqlDbType.Int, SqlDbType.Decimal), ConversionType.Cast },
                { (SqlDbType.SmallInt, SqlDbType.Int), ConversionType.Cast },
                { (SqlDbType.SmallInt, SqlDbType.SmallInt), ConversionType.NoNeed },
                { (SqlDbType.SmallInt, SqlDbType.BigInt), ConversionType.Cast },
                { (SqlDbType.SmallInt, SqlDbType.Float), ConversionType.Cast },
                { (SqlDbType.SmallInt, SqlDbType.NVarChar), ConversionType.Cast },
                { (SqlDbType.SmallInt, SqlDbType.DateTime2), ConversionType.Invalid },
                { (SqlDbType.SmallInt, SqlDbType.Bit), ConversionType.Cast },
                { (SqlDbType.SmallInt, SqlDbType.UniqueIdentifier), ConversionType.Invalid },
                { (SqlDbType.SmallInt, SqlDbType.TinyInt), ConversionType.Cast },
                { (SqlDbType.SmallInt, SqlDbType.Binary), ConversionType.Cast },
                { (SqlDbType.SmallInt, SqlDbType.Decimal), ConversionType.Cast },
                { (SqlDbType.BigInt, SqlDbType.Int), ConversionType.Cast },
                { (SqlDbType.BigInt, SqlDbType.SmallInt), ConversionType.Cast },
                { (SqlDbType.BigInt, SqlDbType.BigInt), ConversionType.NoNeed },
                { (SqlDbType.BigInt, SqlDbType.Float), ConversionType.Cast },
                { (SqlDbType.BigInt, SqlDbType.NVarChar), ConversionType.Cast },
                { (SqlDbType.BigInt, SqlDbType.DateTime2), ConversionType.Invalid },
                { (SqlDbType.BigInt, SqlDbType.Bit), ConversionType.Cast },
                { (SqlDbType.BigInt, SqlDbType.UniqueIdentifier), ConversionType.Invalid },
                { (SqlDbType.BigInt, SqlDbType.TinyInt), ConversionType.Cast },
                { (SqlDbType.BigInt, SqlDbType.Binary), ConversionType.Cast },
                { (SqlDbType.BigInt, SqlDbType.Decimal), ConversionType.Cast },
                { (SqlDbType.Float, SqlDbType.Int), ConversionType.Cast },
                { (SqlDbType.Float, SqlDbType.SmallInt), ConversionType.Cast },
                { (SqlDbType.Float, SqlDbType.BigInt), ConversionType.Cast },
                { (SqlDbType.Float, SqlDbType.Float), ConversionType.NoNeed },
                { (SqlDbType.Float, SqlDbType.NVarChar), ConversionType.Cast },
                { (SqlDbType.Float, SqlDbType.DateTime2), ConversionType.Invalid },
                { (SqlDbType.Float, SqlDbType.Bit), ConversionType.Cast },
                { (SqlDbType.Float, SqlDbType.UniqueIdentifier), ConversionType.Invalid },
                { (SqlDbType.Float, SqlDbType.TinyInt), ConversionType.Cast },
                { (SqlDbType.Float, SqlDbType.Binary), ConversionType.Cast },
                { (SqlDbType.Float, SqlDbType.Decimal), ConversionType.Cast },
                { (SqlDbType.NVarChar, SqlDbType.Int), ConversionType.Cast },
                { (SqlDbType.NVarChar, SqlDbType.SmallInt), ConversionType.Cast },
                { (SqlDbType.NVarChar, SqlDbType.BigInt), ConversionType.Cast },
                { (SqlDbType.NVarChar, SqlDbType.Float), ConversionType.Cast },
                { (SqlDbType.NVarChar, SqlDbType.NVarChar), ConversionType.NoNeed },
                { (SqlDbType.NVarChar, SqlDbType.DateTime2), ConversionType.Cast },
                { (SqlDbType.NVarChar, SqlDbType.Bit), ConversionType.Cast },
                { (SqlDbType.NVarChar, SqlDbType.UniqueIdentifier), ConversionType.Cast },
                { (SqlDbType.NVarChar, SqlDbType.TinyInt), ConversionType.Cast },
                { (SqlDbType.NVarChar, SqlDbType.Binary), ConversionType.Convert },
                { (SqlDbType.NVarChar, SqlDbType.Decimal), ConversionType.Cast },
                { (SqlDbType.DateTime2, SqlDbType.Int), ConversionType.Invalid },
                { (SqlDbType.DateTime2, SqlDbType.SmallInt), ConversionType.Invalid },
                { (SqlDbType.DateTime2, SqlDbType.BigInt), ConversionType.Invalid },
                { (SqlDbType.DateTime2, SqlDbType.Float), ConversionType.Invalid },
                { (SqlDbType.DateTime2, SqlDbType.NVarChar), ConversionType.Cast },
                { (SqlDbType.DateTime2, SqlDbType.DateTime2), ConversionType.NoNeed },
                { (SqlDbType.DateTime2, SqlDbType.Bit), ConversionType.Invalid },
                { (SqlDbType.DateTime2, SqlDbType.UniqueIdentifier), ConversionType.Invalid },
                { (SqlDbType.DateTime2, SqlDbType.TinyInt), ConversionType.Invalid },
                { (SqlDbType.DateTime2, SqlDbType.Binary), ConversionType.Convert },
                { (SqlDbType.DateTime2, SqlDbType.Decimal), ConversionType.Invalid },
                { (SqlDbType.Bit, SqlDbType.Int), ConversionType.Cast },
                { (SqlDbType.Bit, SqlDbType.SmallInt), ConversionType.Cast },
                { (SqlDbType.Bit, SqlDbType.BigInt), ConversionType.Cast },
                { (SqlDbType.Bit, SqlDbType.Float), ConversionType.Cast },
                { (SqlDbType.Bit, SqlDbType.NVarChar), ConversionType.Cast },
                { (SqlDbType.Bit, SqlDbType.DateTime2), ConversionType.Invalid },
                { (SqlDbType.Bit, SqlDbType.Bit), ConversionType.NoNeed },
                { (SqlDbType.Bit, SqlDbType.UniqueIdentifier), ConversionType.Invalid },
                { (SqlDbType.Bit, SqlDbType.TinyInt), ConversionType.Cast },
                { (SqlDbType.Bit, SqlDbType.Binary), ConversionType.Cast },
                { (SqlDbType.Bit, SqlDbType.Decimal), ConversionType.Cast },
                { (SqlDbType.UniqueIdentifier, SqlDbType.Int), ConversionType.Invalid },
                { (SqlDbType.UniqueIdentifier, SqlDbType.SmallInt), ConversionType.Invalid },
                { (SqlDbType.UniqueIdentifier, SqlDbType.BigInt), ConversionType.Invalid },
                { (SqlDbType.UniqueIdentifier, SqlDbType.Float), ConversionType.Invalid },
                { (SqlDbType.UniqueIdentifier, SqlDbType.NVarChar), ConversionType.Cast },
                { (SqlDbType.UniqueIdentifier, SqlDbType.DateTime2), ConversionType.Invalid },
                { (SqlDbType.UniqueIdentifier, SqlDbType.Bit), ConversionType.Invalid },
                { (SqlDbType.UniqueIdentifier, SqlDbType.UniqueIdentifier), ConversionType.NoNeed },
                { (SqlDbType.UniqueIdentifier, SqlDbType.TinyInt), ConversionType.Invalid },
                { (SqlDbType.UniqueIdentifier, SqlDbType.Binary), ConversionType.Cast },
                { (SqlDbType.UniqueIdentifier, SqlDbType.Decimal), ConversionType.Invalid },
                { (SqlDbType.TinyInt, SqlDbType.Int), ConversionType.Cast },
                { (SqlDbType.TinyInt, SqlDbType.SmallInt), ConversionType.Cast },
                { (SqlDbType.TinyInt, SqlDbType.BigInt), ConversionType.Cast },
                { (SqlDbType.TinyInt, SqlDbType.Float), ConversionType.Cast },
                { (SqlDbType.TinyInt, SqlDbType.NVarChar), ConversionType.Cast },
                { (SqlDbType.TinyInt, SqlDbType.DateTime2), ConversionType.Invalid },
                { (SqlDbType.TinyInt, SqlDbType.Bit), ConversionType.Cast },
                { (SqlDbType.TinyInt, SqlDbType.UniqueIdentifier), ConversionType.Invalid },
                { (SqlDbType.TinyInt, SqlDbType.TinyInt), ConversionType.NoNeed },
                { (SqlDbType.TinyInt, SqlDbType.Binary), ConversionType.Cast },
                { (SqlDbType.TinyInt, SqlDbType.Decimal), ConversionType.Cast },
                { (SqlDbType.Binary, SqlDbType.Int), ConversionType.Cast },
                { (SqlDbType.Binary, SqlDbType.SmallInt), ConversionType.Cast },
                { (SqlDbType.Binary, SqlDbType.BigInt), ConversionType.Cast },
                { (SqlDbType.Binary, SqlDbType.Float), ConversionType.Invalid },
                { (SqlDbType.Binary, SqlDbType.NVarChar), ConversionType.Cast },
                { (SqlDbType.Binary, SqlDbType.DateTime2), ConversionType.Convert },
                { (SqlDbType.Binary, SqlDbType.Bit), ConversionType.Cast },
                { (SqlDbType.Binary, SqlDbType.UniqueIdentifier), ConversionType.Cast },
                { (SqlDbType.Binary, SqlDbType.TinyInt), ConversionType.Cast },
                { (SqlDbType.Binary, SqlDbType.Binary), ConversionType.NoNeed },
                { (SqlDbType.Binary, SqlDbType.Decimal), ConversionType.Cast },
                { (SqlDbType.Decimal, SqlDbType.Int), ConversionType.Cast },
                { (SqlDbType.Decimal, SqlDbType.SmallInt), ConversionType.Cast },
                { (SqlDbType.Decimal, SqlDbType.BigInt), ConversionType.Cast },
                { (SqlDbType.Decimal, SqlDbType.Float), ConversionType.Cast },
                { (SqlDbType.Decimal, SqlDbType.NVarChar), ConversionType.Cast },
                { (SqlDbType.Decimal, SqlDbType.DateTime2), ConversionType.Invalid },
                { (SqlDbType.Decimal, SqlDbType.Bit), ConversionType.Cast },
                { (SqlDbType.Decimal, SqlDbType.UniqueIdentifier), ConversionType.Invalid },
                { (SqlDbType.Decimal, SqlDbType.TinyInt), ConversionType.Cast },
                { (SqlDbType.Decimal, SqlDbType.Binary), ConversionType.Cast },
                { (SqlDbType.Decimal, SqlDbType.Decimal), ConversionType.Cast },
            };


        class ExpressionRenderingContext
        {
            public ExpressionRenderingContext()
            {

            }
            public ExpressionRenderingContext(ExpressionRenderingContext ctx)
            {
                ExpectLogicalExpression = ctx.ExpectLogicalExpression;
                EnclosingOperator = ctx.EnclosingOperator;
            }
            public bool ExpectLogicalExpression { get; set; } = false;

            /// <summary>
            /// If in the current rendering context, a logical expression can be used (e.g. a > b)
            /// If it is not expected, we need to convert it to bit type
            /// </summary>
            /// <param name="newVal"></param>
            /// <returns></returns>
            public ExpressionRenderingContext ModifyExpectLogicalExpression(bool newVal)
            {
                ExpectLogicalExpression = newVal;
                return this;
            }
            public LogicalOperator EnclosingOperator { get; set; }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="graphDef"></param>
        /// <param name="queryParams">A set of parameters and its default value</param>
        /// <param name="targetVC">Optional. If provided, a true VC url is provided, or empty, for local testing</param>
        /// <param name="logger">Optional. For logging</param>
        public SQLRenderer
            (
                ISQLDBSchemaProvider graphDef,
                ILoggable logger = null
            )
        {
            _graphDef = graphDef;
            _logger = logger;
        }

        #region Helpers
        private string GetFieldNameForEntityField(string prefix, string singleFieldName)
        {
            var prefixClean = TextHelper.MakeCompliantString(prefix, "[A-Za-z0-9_]+");
            return $"__{prefixClean}_{singleFieldName}";
        }

        /// <summary>
        /// Returns a list of fields that are
        ///    - stand alone single field
        ///    - single field that are part of entity and got referenced
        /// </summary>
        /// <returns></returns>
        private IEnumerable<(string EntityAlias, string PropertyName, Type FieldType, bool IsKeyfield)> ExpandSchema(Schema schema)
        {
            return schema
                .Where(f => f is EntityField).Cast<EntityField>()
                    .SelectMany(ef => ef.ReferencedFieldAliases
                        .Select(fn => (
                            EntityAlias: ef.FieldAlias,
                            PropertyName: fn,
                            FieldType: ef.EncapsulatedFields.First(f2 => fn == f2.FieldAlias).FieldType,
                            IsKeyfield: fn == ef.NodeJoinField?.FieldAlias || fn == ef.RelSourceJoinField?.FieldAlias || fn == ef.RelSinkJoinField?.FieldAlias
                            )))
                .Union(schema
                    .Where(f => f is ValueField).Cast<ValueField>()
                    .Select(fn => (EntityAlias: (string)null, PropertyName: fn.FieldAlias, FieldType: fn.FieldType, IsKeyfield: false)));
        }

        private string EscapeStringLiteral(string originalStr)
        {
            return originalStr.Replace("'", "''");
        }

        private string RenderTypeCastingForExpression(Type targetType, string expr)
        {
            var unboxedType = TypeHelper.GetUnderlyingTypeIfNullable(targetType);
            var sqlTypeText = SQLTypeRenderingMapping[TypeToSQLTypeMapping[unboxedType]];
            if (targetType != unboxedType)
            {
                return $"CAST({expr} AS {sqlTypeText})";
            }
            else
            {
                return $"ISNULL(CAST({expr} AS {sqlTypeText}), {expr})";
            }
        }

        private string RenderTypeConversionForExpression(Type targetType, string expr)
        {
            var unboxedType = TypeHelper.GetUnderlyingTypeIfNullable(targetType);
            var sqlTypeText = SQLTypeRenderingMapping[TypeToSQLTypeMapping[unboxedType]];
            return $"CONVERT({sqlTypeText}, {expr})";
        }

        private string RenderCaseValueExpression(Type targetType, QueryExpression expr, ExpressionRenderingContext exprCtx)
        {
            /// References:
            ///   - https://sqlsunday.com/2019/06/05/cast-convert-makes-expressions-nullable/
            ///
            var renderedExpr = RenderExpression(expr, exprCtx);
            var currentType = expr.EvaluateType();

            // first check if the type conversion is allowed
            var fromSQLType = TypeToSQLTypeMapping[TypeHelper.GetUnderlyingTypeIfNullable(currentType)];
            var toSQLType = TypeToSQLTypeMapping[TypeHelper.GetUnderlyingTypeIfNullable(targetType)];
            var conversionType = SQLTypeConversionType[(fromSQLType, toSQLType)];
            switch (conversionType)
            {
                case ConversionType.Cast:
                    return RenderTypeCastingForExpression(targetType, renderedExpr);
                case ConversionType.Convert:
                    return RenderTypeConversionForExpression(targetType, renderedExpr);
                case ConversionType.NoNeed:
                    return renderedExpr;
                case ConversionType.Invalid:
                    throw new TranspilerNotSupportedException($"Converting from type {fromSQLType} to type {toSQLType}");
                default:
                    throw new TranspilerInternalErrorException($"Unexpected conversion type {conversionType}");
            }
        }
        #endregion Helpers

        #region Logical Operator Renderers

        private string RenderDataSource(DataSourceOperator dataSourceOp, int depth)
        {
            var codeSnip = new StringBuilder();

            // Currently we support only single source data source
            Debug.Assert(
                dataSourceOp.OutputSchema.First() is EntityField &&
                dataSourceOp.OutputSchema.Count() == 1
            );

            var ent = dataSourceOp.OutputSchema.First() as EntityField;
            var storDesc = _graphDef.GetSQLTableDescriptors(ent.BoundEntityName);
            var allReferencedFieldsWithAliasPrefix = ent.ReferencedFieldAliases.Select(f => GetFieldNameForEntityField(ent.EntityName, f));
            codeSnip.AppendLine(depth, $"SELECT");

            // Render join key fields always and does it first
            bool isFirstRow = true;
            if (ent.Type == EntityField.EntityType.Node)
            {
                var nodeIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.NodeJoinField.FieldAlias);
                codeSnip.AppendLine(depth + 1, $"{(!isFirstRow ? ", " : " ")}{ent.NodeJoinField.FieldAlias} AS {nodeIdJoinKeyName}");
                isFirstRow = false;
            }
            else
            {
                var edgeSrcIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.RelSourceJoinField.FieldAlias);
                var edgeSinkIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.RelSinkJoinField.FieldAlias);
                codeSnip.AppendLine(depth + 1, $"{(!isFirstRow ? ", " : " ")}{ent.RelSourceJoinField.FieldAlias} AS {edgeSrcIdJoinKeyName}");
                codeSnip.AppendLine(depth + 1, $", {ent.RelSinkJoinField.FieldAlias} AS {edgeSinkIdJoinKeyName}");
                isFirstRow = false;
            }

            // Render other non-joinkey fields
            foreach (var field in ent.ReferencedFieldAliases
                .Where(f => (ent.NodeJoinField?.FieldAlias != f && ent.RelSourceJoinField?.FieldAlias != f && ent.RelSinkJoinField?.FieldAlias != f)))
            {
                var fieldAlias = GetFieldNameForEntityField(ent.FieldAlias, field);
                codeSnip.AppendLine(depth + 1, $", {field} AS {fieldAlias}");
            }

            codeSnip.AppendLine(depth, $"FROM");
            codeSnip.AppendLine(depth + 1, $"{ storDesc.TableOrViewName}");

            return codeSnip.ToString();
        }

        private string RenderJoin(JoinOperator joinOp, int depth)
        {
            var codeSnip = new StringBuilder();
            
            var leftOp = joinOp.InOperatorLeft;
            var rightOp = joinOp.InOperatorRight;
            
            var leftVar = "_left";
            var rightVar = "_right";

            // expand output schema
            var allColsToOutput = ExpandSchema(joinOp.OutputSchema);
            var allOutputEntities = joinOp.OutputSchema.Where(f => f is EntityField).Cast<EntityField>();

            // expand left input schema
            var leftInCols = ExpandSchema(leftOp.OutputSchema);
            var leftInEntityAliases = leftOp.OutputSchema.Where(f => f is EntityField).Select(e => e.FieldAlias);

            // expand right input schema
            var rightInCols = ExpandSchema(rightOp.OutputSchema);
            var rightInEntityAliases = rightOp.OutputSchema.Where(f => f is EntityField).Select(e => e.FieldAlias);

            codeSnip.AppendLine(depth, $"SELECT");

            // render join key fields always and does it first
            bool isFirstRow = true;
            Debug.Assert(allOutputEntities.Count() > 0);
            foreach (var ent in allOutputEntities)
            {
                var isFromLeft = leftInEntityAliases.Contains(ent.FieldAlias); // prefer left side first (sometimes an alias would be in both left and right, such as in OPTIONAL MATCH case)
                Debug.Assert(isFromLeft || rightInEntityAliases.Contains(ent.FieldAlias));
                if (ent.Type == EntityField.EntityType.Node)
                {
                    var nodeIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.NodeJoinField.FieldAlias);
                    codeSnip.AppendLine(depth + 1, $"{(!isFirstRow ? ", " : " ")}{(isFromLeft ? leftVar : rightVar)}.{nodeIdJoinKeyName} AS {nodeIdJoinKeyName}");
                    isFirstRow = false;
                }
                else
                {
                    var edgeSrcIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.RelSourceJoinField.FieldAlias);
                    var edgeSinkIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.RelSinkJoinField.FieldAlias);
                    codeSnip.AppendLine(depth + 1, $"{(!isFirstRow ? ", " : " ")}{(isFromLeft ? leftVar : rightVar)}.{edgeSrcIdJoinKeyName} AS {edgeSrcIdJoinKeyName}");
                    codeSnip.AppendLine(depth + 1, $", {(isFromLeft ? leftVar : rightVar)}.{edgeSinkIdJoinKeyName} AS {edgeSinkIdJoinKeyName}");
                    isFirstRow = false;
                }
            }
            // render selected properties (except key fields, which will always be rendered above)
            foreach (var col in allColsToOutput.Where(c => !c.IsKeyfield))
            {
                var isFromLeft = leftInCols.Any(f => (!string.IsNullOrEmpty(col.EntityAlias) ? f.EntityAlias == col.EntityAlias : true) && f.PropertyName == col.PropertyName);
                Debug.Assert(isFromLeft || rightInCols.Any(f => (!string.IsNullOrEmpty(col.EntityAlias) ? f.EntityAlias == col.EntityAlias : true) && f.PropertyName == col.PropertyName));
                if (!string.IsNullOrEmpty(col.EntityAlias))
                {
                    // entity member (not yet expanded into a single column)
                    var fieldAliasWrappedInEntity = GetFieldNameForEntityField(col.EntityAlias, col.PropertyName);
                    codeSnip.AppendLine(depth + 1, $", {(isFromLeft ? leftVar : rightVar)}.{fieldAliasWrappedInEntity} AS {fieldAliasWrappedInEntity}");
                }
                else
                {
                    // single column reference
                    codeSnip.AppendLine(depth + 1, $", {(isFromLeft ? leftVar : rightVar)}.{col.PropertyName} AS {col.PropertyName}");
                }
            }

            codeSnip.AppendLine(depth, $"FROM (");
            codeSnip.AppendLine(RenderLogicalOperator(leftOp, depth+1));
            codeSnip.AppendLine(depth, $") AS {leftVar}");

            if (joinOp.Type == JoinOperator.JoinType.Cross)
            {
                Debug.Assert(joinOp.JoinPairs.Count == 0);
                codeSnip.AppendLine(depth, $"CROSS JOIN (");
                codeSnip.AppendLine(RenderLogicalOperator(rightOp, depth+1));
                codeSnip.AppendLine(depth, $") AS {rightVar}");
            }
            else
            {
                Debug.Assert(joinOp.Type == JoinOperator.JoinType.Left || joinOp.Type == JoinOperator.JoinType.Inner);
                Debug.Assert(joinOp.JoinPairs.Count > 0);
                codeSnip.AppendLine(depth, $"{(joinOp.Type == JoinOperator.JoinType.Inner ? "INNER JOIN" : "LEFT JOIN")} (");
                codeSnip.AppendLine(RenderLogicalOperator(rightOp, depth+1));
                codeSnip.AppendLine(depth, $") AS {rightVar} ON");                    

                bool isFirstJoinCond = true;
                foreach (var joinKeyPair in joinOp.JoinPairs)
                {
                    var isNodeFromLeft = leftInEntityAliases.Contains(joinKeyPair.NodeAlias);
                    Debug.Assert(isNodeFromLeft ?
                        rightInEntityAliases.Contains(joinKeyPair.RelationshipOrNodeAlias) :
                        leftInEntityAliases.Contains(joinKeyPair.RelationshipOrNodeAlias) && rightInEntityAliases.Contains(joinKeyPair.NodeAlias));
                    var varWithNode = isNodeFromLeft ? leftVar : rightVar;
                    var varWithNodeEntity = joinOp.OutputSchema.First(n => n.FieldAlias == joinKeyPair.NodeAlias) as EntityField;
                    var nodeJoinKey = GetFieldNameForEntityField(joinKeyPair.NodeAlias, varWithNodeEntity.NodeJoinField.FieldAlias);

                    string nodeOrRelJoinKey;
                    var varWithRelOrNode = isNodeFromLeft ? rightVar : leftVar;
                    var varWithRelOrNodeEntity = joinOp.OutputSchema.First(n => n.FieldAlias == joinKeyPair.RelationshipOrNodeAlias) as EntityField;

                    switch (joinKeyPair.Type)
                    {
                        case JoinOperator.JoinKeyPair.JoinKeyPairType.Source:
                            nodeOrRelJoinKey = GetFieldNameForEntityField(joinKeyPair.RelationshipOrNodeAlias, varWithRelOrNodeEntity.RelSourceJoinField.FieldAlias);
                            codeSnip.AppendLine(depth + 1, $"{(isFirstJoinCond ? "" : "AND ")}{varWithNode}.{nodeJoinKey} = {varWithRelOrNode}.{nodeOrRelJoinKey}");
                            break;
                        case JoinOperator.JoinKeyPair.JoinKeyPairType.Sink:
                            nodeOrRelJoinKey = GetFieldNameForEntityField(joinKeyPair.RelationshipOrNodeAlias, varWithRelOrNodeEntity.RelSinkJoinField.FieldAlias);
                            codeSnip.AppendLine(depth + 1, $"{(isFirstJoinCond ? "" : "AND ")}{varWithNode}.{nodeJoinKey} = {varWithRelOrNode}.{nodeOrRelJoinKey}");
                            break;
                        case JoinOperator.JoinKeyPair.JoinKeyPairType.Both:
                            nodeOrRelJoinKey = GetFieldNameForEntityField(joinKeyPair.RelationshipOrNodeAlias, varWithRelOrNodeEntity.RelSourceJoinField.FieldAlias);
                            codeSnip.AppendLine(depth + 1, $"{(isFirstJoinCond ? "" : "AND ")}{varWithNode}.{nodeJoinKey} = {varWithRelOrNode}.{nodeOrRelJoinKey}");
                            nodeOrRelJoinKey = GetFieldNameForEntityField(joinKeyPair.RelationshipOrNodeAlias, varWithRelOrNodeEntity.RelSinkJoinField.FieldAlias);
                            codeSnip.AppendLine(depth + 1, $"{"AND "}{varWithNode}.{nodeJoinKey} == {varWithRelOrNode}.{nodeOrRelJoinKey}");
                            break;
                        case JoinOperator.JoinKeyPair.JoinKeyPairType.NodeId:
                            nodeOrRelJoinKey = GetFieldNameForEntityField(joinKeyPair.RelationshipOrNodeAlias, varWithRelOrNodeEntity.NodeJoinField.FieldAlias);
                            codeSnip.AppendLine(depth + 1, $"{(isFirstJoinCond ? "" : "AND ")}{varWithNode}.{nodeJoinKey} = {varWithRelOrNode}.{nodeOrRelJoinKey}");
                            break;
                        default:
                            Debug.Assert(joinKeyPair.Type == JoinOperator.JoinKeyPair.JoinKeyPairType.Either);
                            var nodeField = joinOp.InputSchema.First(f => f.FieldAlias == joinKeyPair.NodeAlias) as EntityField;
                            var relField = joinOp.InputSchema.First(f => f.FieldAlias == joinKeyPair.RelationshipOrNodeAlias) as EntityField;
                            var isSrc = relField.BoundSourceEntityName == nodeField.BoundEntityName;
                            Debug.Assert(isSrc || relField.BoundSinkEntityName == nodeField.BoundEntityName);
                            nodeOrRelJoinKey = GetFieldNameForEntityField(joinKeyPair.RelationshipOrNodeAlias, isSrc ? varWithRelOrNodeEntity.RelSourceJoinField.FieldAlias : varWithRelOrNodeEntity.RelSinkJoinField.FieldAlias);
                            codeSnip.AppendLine(depth + 1, $"{(isFirstJoinCond ? "" : "AND ")}{varWithNode}.{nodeJoinKey} = {varWithRelOrNode}.{nodeOrRelJoinKey}");
                            break;
                    }

                    isFirstJoinCond = false;
                }
            }

            return codeSnip.ToString();
        }

        private string RenderBinaryOperator(BinaryOperatorInfo op, QueryExpression left, QueryExpression right, ExpressionRenderingContext exprCtx)
        {
            if (op.Name == BinaryOperator.Invalid)
            {
                throw new TranspilerInternalErrorException("Encountered an invalid operator");
            }
            var pattern = OperatorRenderPattern[op.Name] ??
                throw new TranspilerNotSupportedException($"Operator {op.Name}");
            var leftExpr = RenderExpression(left, exprCtx);
            var rightExpr = RenderExpression(right, exprCtx);
            return string.Format(pattern, leftExpr, rightExpr);
        }

        private string RenderFunction(FunctionInfo func, IEnumerable<QueryExpression> parameters, ExpressionRenderingContext exprCtx)
        {
            var exprRenderResult = parameters.Select(p => RenderExpression(p, exprCtx));
            switch (func.FunctionName)
            {
                case Function.ToFloat:
                    Debug.Assert(parameters.Count() == 1);
                    return RenderTypeCastingForExpression(typeof(float), exprRenderResult.First());
                case Function.ToString:
                    Debug.Assert(parameters.Count() == 1);
                    return RenderTypeCastingForExpression(typeof(string), exprRenderResult.First());
                case Function.ToBoolean:
                    Debug.Assert(parameters.Count() == 1);
                    return RenderTypeCastingForExpression(typeof(bool), exprRenderResult.First());
                case Function.ToInteger:
                    Debug.Assert(parameters.Count() == 1);
                    return RenderTypeCastingForExpression(typeof(int), exprRenderResult.First());
                case Function.ToDouble:
                    Debug.Assert(parameters.Count() == 1);
                    return RenderTypeCastingForExpression(typeof(double), exprRenderResult.First());
                case Function.ToLong:
                    Debug.Assert(parameters.Count() == 1);
                    return RenderTypeCastingForExpression(typeof(long), exprRenderResult.First());
                case Function.Not:
                    Debug.Assert(parameters.Count() == 1);
                    return $"NOT ({string.Join(", ", exprRenderResult)})";
                case Function.StringStartsWith:
                    Debug.Assert(parameters.Count() == 2);
                    return $"LEFT({exprRenderResult.First()}, LEN({exprRenderResult.Skip(1).First()})) = {exprRenderResult.Skip(1).First()}";
                case Function.StringEndsWith:
                    Debug.Assert(parameters.Count() == 2);
                    return $"RIGHT({exprRenderResult.First()}, LEN({exprRenderResult.Skip(1).First()})) = {exprRenderResult.Skip(1).First()}";
                case Function.StringContains:
                    Debug.Assert(parameters.Count() == 2);
                    return $"charindex({exprRenderResult.Skip(1).First()}, {exprRenderResult.First()}) >= 1";
                case Function.StringLeft:
                    Debug.Assert(parameters.Count() == 2);
                    return $"LEFT({exprRenderResult.First()}, {exprRenderResult.Skip(1).First()})";
                case Function.StringRight:
                    Debug.Assert(parameters.Count() == 2);
                    return $"RIGHT({exprRenderResult.First()}, {exprRenderResult.Skip(1).First()})";
                case Function.StringTrim:
                    Debug.Assert(parameters.Count() == 1);
                    return $"TRIM({exprRenderResult.First()})";
                case Function.StringLTrim:
                    Debug.Assert(parameters.Count() == 1);
                    return $"LTRIM({exprRenderResult.First()})";
                case Function.StringRTrim:
                    Debug.Assert(parameters.Count() == 1);
                    return $"RTRIM({exprRenderResult.First()})";
                case Function.StringToUpper:
                    Debug.Assert(parameters.Count() == 1);
                    return $"UPPER({exprRenderResult.First()})";
                case Function.StringToLower:
                    Debug.Assert(parameters.Count() == 1);
                    return $"LOWER({exprRenderResult.First()})";
                case Function.StringSize:
                    Debug.Assert(parameters.Count() == 1);
                    return $"LEN({exprRenderResult.First()})";
                case Function.IsNull:
                    return $"({string.Join(", ", exprRenderResult)}) IS NULL";
                case Function.IsNotNull:
                    return $"({string.Join(", ", exprRenderResult)}) IS NOT NULL";
                default:
                    throw new TranspilerNotSupportedException($"Function '{func.FunctionName}'");
            }
        }

        private string RenderExpression(QueryExpression expr, ExpressionRenderingContext exprCtx)
        {
            if (expr is QueryExpressionBinary)
            {
                var exprTyped = expr as QueryExpressionBinary;
                var opType = exprTyped.Operator.Type;
                var exprCtxChild = new ExpressionRenderingContext(exprCtx)
                    .ModifyExpectLogicalExpression(opType == BinaryOperatorType.Logical);
                string exprText = RenderBinaryOperator(
                    exprTyped.Operator,
                    exprTyped.LeftExpression,
                    exprTyped.RightExpression,
                    exprCtxChild
                    );
                if (!exprCtx.ExpectLogicalExpression && 
                    (opType == BinaryOperatorType.Comparison || opType == BinaryOperatorType.Logical))
                {
                    return $"(CASE WHEN {exprText} THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END)";
                }
                else
                {
                    return exprText;
                }
            }
            else if (expr is QueryExpressionAggregationFunction)
            {
                var exprTyped = expr as QueryExpressionAggregationFunction;

                // TODO: temporary block for some yet to be supported aggregation functions
                if (exprTyped.AggregationFunction == AggregationFunction.PercentileCont ||
                    exprTyped.AggregationFunction == AggregationFunction.PercentileDisc)
                {
                    throw new TranspilerNotSupportedException($"Yet to implemented aggregation function {exprTyped.AggregationFunction}");
                }

                // special handling for count
                if (exprTyped.AggregationFunction == AggregationFunction.Count)
                {
                    if (exprTyped.InnerExpression is QueryExpressionProperty &&
                        (exprTyped.InnerExpression as QueryExpressionProperty).Entity != null)
                    {
                        // special handling for Count(entity) or Count(distinct(entity))
                        var innerPropExpr = exprTyped.InnerExpression as QueryExpressionProperty;
                        var entity = innerPropExpr.Entity;
                        if (entity is RelationshipEntity && exprTyped.IsDistinct)
                        {
                            // block a scenario we currently cannot support
                            throw new TranspilerNotSupportedException("COUNT DISTINCT applied to relationship entity");
                        }
                        var entityField = exprCtx.EnclosingOperator.InputSchema.First(f => f.FieldAlias == entity.Alias) as EntityField;
                        Debug.Assert(entityField != null);

                        // we use the key field as surrogate for counting entities
                        var surrogateFieldForCounting = GetFieldNameForEntityField(
                            innerPropExpr.VariableName,
                            entity is RelationshipEntity ? entityField.RelSourceJoinField.FieldAlias : entityField.NodeJoinField.FieldAlias
                            );
                        return $"COUNT({(exprTyped.IsDistinct ? "DISTINCT(" : "")}{surrogateFieldForCounting}{(exprTyped.IsDistinct ? ")" : "")})";
                    }
                    else
                    {
                        // default handling of count
                        if (exprTyped.InnerExpression.GetChildrenQueryExpressionType<QueryExpressionAggregationFunction>().Count() > 0)
                        {
                            throw new TranspilerNotSupportedException("Aggregation function inside aggregate function");
                        }
                        var innerExprStr = RenderExpression(exprTyped.InnerExpression, exprCtx);
                        return $"COUNT({(exprTyped.IsDistinct ? "DISTINCT(" : "")}{innerExprStr}{(exprTyped.IsDistinct ? ")" : "")})";
                    }
                }
                else
                {
                    // default handling
                    if (exprTyped.InnerExpression.GetChildrenQueryExpressionType<QueryExpressionAggregationFunction>().Count() > 0)
                    {
                        throw new TranspilerNotSupportedException("Aggregation function inside aggregate function");
                    }
                    if (exprTyped.IsDistinct)
                    {
                        throw new TranspilerNotSupportedException("Distinct applied to aggregation functions other than COUNT");
                    }
                    var innerExprStr = RenderExpression(exprTyped.InnerExpression, exprCtx);
                    return string.Format(AggregationFunctionRenderPattern[exprTyped.AggregationFunction], innerExprStr);
                }
            }
            else if (expr is QueryExpressionFunction)
            {
                var exprTyped = expr as QueryExpressionFunction;
                var allExprs = (new List<QueryExpression>() { exprTyped.InnerExpression })
                    .Union(exprTyped.AdditionalExpressions ?? Enumerable.Empty<QueryExpression>());
                return RenderFunction(exprTyped.Function, allExprs, exprCtx);
            }
            else if (expr is QueryExpressionProperty)
            {
                var exprTyped = expr as QueryExpressionProperty;
                var expressionText = string.IsNullOrEmpty(exprTyped.PropertyName) ?
                    exprTyped.VariableName :
                    GetFieldNameForEntityField(exprTyped.VariableName, exprTyped.PropertyName);
                return expressionText;
            }
            else if (expr is QueryExpressionList)
            {
                var exprTyped = expr as QueryExpressionList;
                var exprRendered = exprTyped.ExpressionList.Select(e => RenderExpression(e, exprCtx)).ToList();
                return $"({string.Join(", ", exprRendered)})";
            }
            else if (expr is QueryExpressionValue)
            {
                var exprTyped = expr as QueryExpressionValue;
                if (exprTyped.ValueType == typeof(string))
                {
                    // add double quote for string value, and escape if needed
                    return $"'{EscapeStringLiteral(exprTyped.StringValue)}'";
                }
                if (exprTyped.ValueType == typeof(bool))
                {
                    return exprTyped.StringValue.ToLower();
                }
                else
                {
                    return $"{exprTyped.StringValue}";
                }
            }
            else if (expr is QueryExpressionWithAlias)
            {
                throw new NotSupportedException("Does not support aliased expression at non-root level");
            }
            else if (expr is QueryExpressionCaseExpression)
            {
                var caseExprText = RenderCaseExpression(expr as QueryExpressionCaseExpression, exprCtx);
                return caseExprText;
            }
            else
            {
                throw new NotSupportedException($"Unsupported expression type: {expr.GetType().ToString()}");
            }
        }

        private string RenderCaseExpression(QueryExpressionCaseExpression caseExpression, ExpressionRenderingContext exprCtx)
        {
            var caseAlternatives = caseExpression.CaseAlternatives;
            var elseCondition = caseExpression.ElseExpression;
            var targetType = caseExpression.EvaluateType();

            var codeSnip = new StringBuilder();


            // Note: right now the code renderer does not support casing on an expression. The
            //       expression has to be put into the WHEN / ELSE statement like SQL. In future, 
            //       we may support this by embed CASE expr into the WHEN exprs
            if (caseExpression.InitialCaseExpression != null)
            {
                throw new TranspilerNotSupportedException("Please use CASE WHEN <EXPR> ... instead of CASE <EXPR> WHEN <EXPR>. The latter is");
            }

            if (caseAlternatives.Count == 0)
            {
                throw new TranspilerInternalErrorException("No casing statements provided for CASE");
            }

            codeSnip.Append("CASE ");
            Debug.Assert(exprCtx.ExpectLogicalExpression == false);
            foreach (var alterExpr in caseAlternatives)
            {
                codeSnip.Append($"WHEN {RenderExpression(alterExpr.WhenExpression, new ExpressionRenderingContext(exprCtx).ModifyExpectLogicalExpression(true))} ");
                codeSnip.Append($"THEN {RenderCaseValueExpression(targetType, alterExpr.ThenExpression, exprCtx)}");
            }
            if (elseCondition != null)
            {
                codeSnip.Append($" ELSE {RenderCaseValueExpression(targetType, elseCondition, exprCtx)} END");
            }
            else
            {
                codeSnip.Append($" END");
            }

            return codeSnip.ToString();
        }


        private string RenderFilterExpression(QueryExpression filterExpr, LogicalOperator enclosingOp, int depth)
        {
            // Render just the WHERE part
            var codeSnip = new StringBuilder();
            var condText = RenderExpression(
                filterExpr,
                new ExpressionRenderingContext()
                {
                    ExpectLogicalExpression = true, // SQL does not allow value type to be in WHERE condition
                        EnclosingOperator = enclosingOp
                });
            codeSnip.AppendLine(depth, $"WHERE");
            codeSnip.AppendLine(depth + 1, $"{condText}");

            return codeSnip.ToString();
        }

        private string RenderOrderbyClause(IList<SortItem> orderByClause, LogicalOperator enclosingOp, int depth)
        {
            // Render just the Order By part
            var codeSnip = new StringBuilder();

            codeSnip.AppendLine(depth, $"ORDER BY");
            var isFirstRowOrderBy = true;
            foreach (var orderExpr in orderByClause)
            {
                bool isDescending = orderExpr.IsDescending;
                var exprText = RenderExpression(
                    orderExpr.InnerExpression,
                    new ExpressionRenderingContext()
                    {
                        ExpectLogicalExpression = false,
                        EnclosingOperator = enclosingOp
                    });
                codeSnip.AppendLine(depth + 1, $"{(isFirstRowOrderBy ? "" : ", ")}{exprText} {(isDescending ? "DESC" : "ASC")}");
                isFirstRowOrderBy = false;
            }

            return codeSnip.ToString();
        }

        private string RenderProjection(ProjectionOperator prjOp, int depth)
        {
            // Renders a Projection operator, or a Projection + Selection operator if applicable
            var codeSnip = new StringBuilder();
            
            var allColsToOutput = ExpandSchema(prjOp.OutputSchema);
            var allOutputEntities = prjOp.OutputSchema.Where(f => f is EntityField).Cast<EntityField>();
            var allOutputSingleFields = prjOp.OutputSchema.Where(f => f is ValueField).Cast<ValueField>();

            // look ahead one more operator - if it is selection operator we will collapse with the projection
            var preCond = prjOp.InOperator as SelectionOperator;
            var topXVal = preCond?.Limit?.RowCount;
            // extra debug check: schema has not changed by selection operator (which should have been guaranteed by logical pan)
            Debug.Assert(preCond == null || preCond.InputSchema.Count == preCond.OutputSchema.Count && preCond.OutputSchema.Count == prjOp.InputSchema.Count);

            codeSnip.AppendLine(depth, $"SELECT{(prjOp.IsDistinct ? " DISTINCT" : "")}{(topXVal.HasValue ? $" TOP {topXVal.Value}" : "")}");

            // project entities and flow the join keys
            // do group by if any aggregation functions used
            var nonAggFieldExprs = new List<string>();

            bool isFirstRow = true;
            foreach (var ent in allOutputEntities)
            {
                var entExpr = prjOp.ProjectionMap[ent.FieldAlias];
                Debug.Assert(entExpr is QueryExpressionProperty);
                var inSchemaAlias = (entExpr as QueryExpressionProperty).VariableName;

                // project keys for entities that are exposed by the projection
                if (ent.Type == EntityField.EntityType.Node)
                {
                    var nodeIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.NodeJoinField.FieldAlias);
                    var nodeIdInSchemaJoinKeyName = GetFieldNameForEntityField(inSchemaAlias, ent.NodeJoinField.FieldAlias);
                    codeSnip.AppendLine(depth+1, $"{(!isFirstRow ? ", " : " ")}{nodeIdInSchemaJoinKeyName} AS {nodeIdJoinKeyName}");
                    nonAggFieldExprs.Add(nodeIdInSchemaJoinKeyName);
                    isFirstRow = false;
                }
                else
                {
                    var edgeSrcIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.RelSourceJoinField.FieldAlias);
                    var edgeSinkIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.RelSinkJoinField.FieldAlias);
                    var edgeSrcIdInSchemaJoinKeyName = GetFieldNameForEntityField(inSchemaAlias, ent.RelSourceJoinField.FieldAlias);
                    var edgeSinkIdInSchemaJoinKeyName = GetFieldNameForEntityField(inSchemaAlias, ent.RelSinkJoinField.FieldAlias);
                    codeSnip.AppendLine(depth + 1, $"{(!isFirstRow ? ", " : " ")}{edgeSrcIdInSchemaJoinKeyName} AS {edgeSrcIdJoinKeyName}");
                    codeSnip.AppendLine(depth + 1, $", {edgeSinkIdInSchemaJoinKeyName} AS {edgeSinkIdJoinKeyName}");
                    nonAggFieldExprs.Add(edgeSrcIdInSchemaJoinKeyName);
                    nonAggFieldExprs.Add(edgeSinkIdInSchemaJoinKeyName);
                    isFirstRow = false;
                }

                // referenced non-joinkey fields in the wrapped entities
                var nonNullJoinKeyValues = new string[] { ent.NodeJoinField?.FieldAlias, ent.RelSourceJoinField?.FieldAlias, ent.RelSinkJoinField?.FieldAlias }.Where(a => !string.IsNullOrEmpty(a));
                foreach (var field in ent.ReferencedFieldAliases.Except(nonNullJoinKeyValues))
                {
                    var inSchemaFieldName = GetFieldNameForEntityField(inSchemaAlias, field);
                    var outSchemaFieldName = GetFieldNameForEntityField(ent.FieldAlias, field);
                    codeSnip.AppendLine(depth + 1, $", {inSchemaFieldName} AS {outSchemaFieldName}");
                    nonAggFieldExprs.Add(inSchemaFieldName);
                }
            }

            // project single fields
            foreach (var field in allOutputSingleFields)
            {
                // find corresponding expression
                var expr = prjOp.ProjectionMap[field.FieldAlias];
                var exprText = RenderExpression(
                    expr,
                    new ExpressionRenderingContext()
                    {
                        ExpectLogicalExpression = false,
                        EnclosingOperator = prjOp
                    });
                codeSnip.AppendLine(depth + 1, $"{(!isFirstRow ? ", " : " ")}{exprText} AS {field.FieldAlias}");
                if (expr.GetChildrenQueryExpressionType<QueryExpressionAggregationFunction>().Count() == 0)
                {
                    nonAggFieldExprs.Add(exprText);
                }
                isFirstRow = false;
            }

            codeSnip.AppendLine(depth, $"FROM (");

            
            // collapse the selection with the current projection if we can
            if (preCond != null)
            {
                var prevOp = preCond.InOperator;
                codeSnip.AppendLine(RenderLogicalOperator(prevOp, depth + 1));
                codeSnip.AppendLine(depth, $") AS _proj");

                // if there is any filtering, render it as WHERE
                if (preCond?.FilterExpression != null)
                {
                    codeSnip.AppendLine(depth, RenderFilterExpression(preCond.FilterExpression, preCond, depth));
                }
            }
            else
            {
                codeSnip.AppendLine(RenderLogicalOperator(prjOp.InOperator, depth + 1));
                codeSnip.AppendLine(depth, $") AS _proj");
            }
            
            // add group by if aggregation is used and there are non aggregation columns
            if (prjOp.HasAggregationField && nonAggFieldExprs.Count > 0)
            {
                codeSnip.AppendLine(depth, $"GROUP BY");
                var isFirstRowGrpBy = true;
                foreach (var field in nonAggFieldExprs)
                {
                    codeSnip.AppendLine(depth+1, $"{(isFirstRowGrpBy ? "" : ", ")}{field}");
                    isFirstRowGrpBy = false;
                }
            }

            // render the optional ORDER BY
            if ((preCond?.OrderByExpressions?.Count() ?? 0) > 0)
            {
                codeSnip.Append(RenderOrderbyClause(preCond.OrderByExpressions, preCond, depth));
            }

            return codeSnip.ToString();
        }

        private string RenderSelection(SelectionOperator condOp, int depth)
        {
            var codeSnip = new StringBuilder();
            
            // Assert that schema has not changed and matches
            Debug.Assert(condOp.InputSchema.Count == condOp.OutputSchema.Count);

            // expand output schema
            var allColsToOutput = ExpandSchema(condOp.OutputSchema);
            var allOutputEntities = condOp.OutputSchema.Where(f => f is EntityField).Cast<EntityField>();
            var allOutputSingleFields = condOp.OutputSchema.Where(f => f is ValueField).Cast<ValueField>();
            var topXVal = condOp.Limit?.RowCount;
            bool isFirstRow = true;

            codeSnip.AppendLine(depth, $"SELECT {(topXVal.HasValue ? $" TOP {topXVal.Value}" : "")}");

            // project entities and join keys need to be flowed
            foreach (var ent in allOutputEntities)
            {
                // project keys for entities that are exposed by the projection
                if (ent.Type == EntityField.EntityType.Node)
                {
                    var nodeIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.NodeJoinField.FieldAlias);
                    codeSnip.AppendLine(depth+1, $"{(!isFirstRow ? ", " : " ")}{nodeIdJoinKeyName} AS {nodeIdJoinKeyName}");
                    isFirstRow = false;
                }
                else
                {
                    var edgeSrcIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.RelSourceJoinField.FieldAlias);
                    var edgeSinkIdJoinKeyName = GetFieldNameForEntityField(ent.FieldAlias, ent.RelSinkJoinField.FieldAlias);
                    codeSnip.AppendLine(depth + 1, $"{(!isFirstRow ? ", " : " ")}{edgeSrcIdJoinKeyName} AS {edgeSrcIdJoinKeyName}");
                    codeSnip.AppendLine(depth + 1, $", {edgeSinkIdJoinKeyName} AS {edgeSinkIdJoinKeyName}");
                    isFirstRow = false;
                }

                // referenced fields in the wrapped entities
                var nonNullJoinKeyValues = new string[] { ent.NodeJoinField?.FieldAlias, ent.RelSourceJoinField?.FieldAlias, ent.RelSinkJoinField?.FieldAlias }.Where(a => !string.IsNullOrEmpty(a));
                foreach (var field in ent.ReferencedFieldAliases.Except(nonNullJoinKeyValues))
                {
                    var fieldName = GetFieldNameForEntityField(ent.FieldAlias, field);
                    codeSnip.AppendLine(depth + 1, $", {fieldName} AS {fieldName}");
                }
            }

            // project single properties (columns)
            foreach (var field in allOutputSingleFields)
            {
                // find corresponding expression
                codeSnip.AppendLine(depth + 1, $"{(!isFirstRow ? ", " : " ")}{field.FieldAlias} AS {field.FieldAlias}");
                isFirstRow = false;
            }

            codeSnip.AppendLine(depth, $"FROM (");
            codeSnip.AppendLine(RenderLogicalOperator(condOp.InOperator, depth+1));
            codeSnip.AppendLine(depth, $") AS _select");

            // if there is any filtering, render it as WHERE
            if (condOp.FilterExpression != null)
            {
                codeSnip.AppendLine(depth, RenderFilterExpression(condOp.FilterExpression, condOp, depth));
            }

            // render the optional ORDER BY
            if ((condOp.OrderByExpressions?.Count() ?? 0) > 0)
            {
                codeSnip.Append(RenderOrderbyClause(condOp.OrderByExpressions, condOp, depth));
            }

            return codeSnip.ToString();
        }

        private string RenderSet(SetOperator opSel, int depth)
        {
            var codeSnip = new StringBuilder();
            codeSnip.AppendLine(depth, "(");
            codeSnip.AppendLine(RenderLogicalOperator(opSel.InOperatorLeft, depth + 1));
            codeSnip.AppendLine(depth, $") {(opSel.SetOperation == SetOperator.SetOperationType.UnionAll ? "UNION ALL" : "UNION")}");
            codeSnip.AppendLine(depth, "(");
            codeSnip.AppendLine(RenderLogicalOperator(opSel.InOperatorRight, depth + 1));
            codeSnip.AppendLine(depth, ")");

            return codeSnip.ToString();
        }


        /// <summary>
        /// Called by subclass to render main portion of the code
        /// </summary>
        /// <param name="logicalPlan"></param>
        /// <returns></returns>
        private string RenderLogicalOperator(LogicalOperator op, int depth)
        {
            switch (op)
            {
                case DataSourceOperator opDs:
                    return RenderDataSource(opDs, depth);
                case ProjectionOperator opProj:
                    return RenderProjection(opProj, depth);
                case SelectionOperator opSel:
                    return RenderSelection(opSel, depth);
                case JoinOperator opSel:
                    return RenderJoin(opSel, depth);
                case SetOperator opSet:
                    return RenderSet(opSet, depth);
                default:
                    throw new TranspilerInternalErrorException($"Unexpected operator type: {op.GetType().Name}");
            }
        }

        #endregion Logical Operator Renderers

        /// <summary>
        /// Render the Scope code for a given plan and return the output variable names
        /// </summary>
        /// <param name="logicalPlan"></param>
        /// <param name="finalOutputVars"></param>
        /// <returns></returns>
        public string RenderPlan(LogicalPlan logicalPlan)
        {
            var codeScript = new StringBuilder();

            if (logicalPlan.TerminalOperators.Count() != 1)
            {
                throw new TranspilerNotSupportedException("Multi-output query");
            }

            var terminatingOperator = logicalPlan.TerminalOperators.First();

            var queryCodeText = RenderLogicalOperator(terminatingOperator, 0);
            return queryCodeText;
        }

    }
}
