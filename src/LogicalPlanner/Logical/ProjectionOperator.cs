/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.openCypherParser.AST;

namespace openCypherTranspiler.LogicalPlanner
{
    /// <summary>
    /// Operator to satisfy SELECT
    /// </summary>
    public sealed class ProjectionOperator : UnaryLogicalOperator
    {
        public bool IsDistinct { get; private set; }
        public bool HasAggregationField { get; private set; } = false;
        public ProjectionOperator(LogicalOperator inOp, IDictionary<string, QueryExpression> projectionMap, bool isDistinct)
        {
            ProjectionMap = projectionMap;
            IsDistinct = isDistinct;
            SetInOperator(inOp);
        }

        // projection map, from source expression to projection result (indexed by its FieldName)
        public IDictionary<string, QueryExpression> ProjectionMap { get; private set; }

        internal override void PropagateDateTypesForOutSchema()
        {
            // projection may alter the schema with calculated columns
            // we calculate the data type of all the fields in the output schema
            // using type evaluation method on the QueryExpression

            // Map from out_alias to { projection_expression, 
            // e.g.: (a.b + c) AS d
            //    "d" -> (<expression of a.b+c>, <d's field in OutputSchema>)
            var exprToOutputMap = ProjectionMap.ToDictionary(
                kv => kv.Key, // key is output alias
                kv => new { Expr = kv.Value, Field = OutputSchema.First(f => f.FieldAlias == kv.Key) } // value is the corresponding field object and expression
                );
            foreach (var map in exprToOutputMap)
            {
                // toggle the fact if any of the output column requires aggregation
                HasAggregationField = HasAggregationField || 
                    (map.Value.Expr.GetChildrenQueryExpressionType<QueryExpressionAggregationFunction>().Count() > 0);

                var allPropertyReferences = map.Value.Expr.GetChildrenQueryExpressionType<QueryExpressionProperty>();

                // update types for all QueryExpressionPropty object first based on InputSchema (so QueryExpression.EvaluteType() will work)
                foreach (var prop in allPropertyReferences)
                {
                    UpdatePropertyBasedOnAliasFromInputSchema(prop);
                }

                // then, update the type in the OutputSchema
                if (map.Value.Field is EntityField)
                {
                    // This can only be direct exposure of entity (as opposed to deference of a particular property)
                    // We just copy of the fields that the entity can potentially be dereferenced
                    Debug.Assert(allPropertyReferences.Count() == 1);
                    var varName = allPropertyReferences.First().VariableName;
                    var matchInputField = InputSchema.First(f => f.FieldAlias == varName);
                    map.Value.Field.Copy(matchInputField);
                }
                else
                {
                    // This can be a complex expression involve multiple field/column references
                    // We will compute the type of the expression
                    Debug.Assert(map.Value.Field is ValueField);
                    var evalutedType = map.Value.Expr.EvaluateType();
                    var outField = map.Value.Field as ValueField;
                    outField.FieldType = evalutedType;
                }
            }
        }

        internal override void AppendReferencedProperties(IDictionary<string, EntityField> entityFields)
        {
            // NOOP. For projection, all referenced properties are in output schema already and no extra need to be added.
        }
    }
}
