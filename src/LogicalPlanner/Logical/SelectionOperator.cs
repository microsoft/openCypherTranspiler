/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.openCypherParser.AST;
using openCypherTranspiler.openCypherParser.Common;

namespace openCypherTranspiler.LogicalPlanner
{
    /// <summary>
    /// Operator for row selections (e.g. WHERE or LIMIT)
    /// We also captures ORDERING instructions in this operator, too
    /// </summary>
    public sealed class SelectionOperator : UnaryLogicalOperator
    {
        public SelectionOperator(LogicalOperator inOp, QueryExpression filterExpr)
        {
            FilterExpression = filterExpr;
            UnexpandedEntityInequalityConditions = null;
            SetInOperator(inOp);
        }
        public SelectionOperator(LogicalOperator inOp,IList<SortItem> orderExprs, LimitClause limit)
        {
            FilterExpression = null;
            UnexpandedEntityInequalityConditions = null;
            SetInOperator(inOp);
            OrderByExpressions = orderExprs;
            Limit = limit;
        }

        public SelectionOperator(LogicalOperator inOp, IEnumerable<(string, string)> entityInequityConds)
        {
            FilterExpression = null;
            UnexpandedEntityInequalityConditions = entityInequityConds.ToList();
            SetInOperator(inOp);
        }

        public QueryExpression FilterExpression { get; private set; }

        /// <summary>
        /// use to store ORDER BY expression under WITH or RETURN clause
        /// </summary>
        public IList<SortItem> OrderByExpressions { get; private set; }

        /// <summary>
        /// use to store LIMIT expression under WITH or RETURN clause
        /// </summary>
        public LimitClause Limit { get; private set; }

        /// <summary>
        /// Inequality conditions to be added after binding, specifically reserved for the implied inequality
        /// conditions from MATCH clauses like MATCH (p:Person)-[a1:Acted_In]-(m:Movie)-[a2:Acted_In]-(p2:Person)
        /// where implicit condition of a1 <> a2 must be added
        /// </summary>
        public IEnumerable<(string RelAlias1, string RelAlias2)> UnexpandedEntityInequalityConditions { get; private set; }

        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.AppendLine($"Filtering Condition: {FilterExpression};");
            return sb.ToString();
        }
        
        internal override void PropagateDateTypesForOutSchema()
        {
            // Select operator does not alter schema, so the output should map to input perfectly
            Debug.Assert(OutputSchema.All(f => InputSchema.Any(f2 => f2.FieldAlias == f.FieldAlias && f2.GetType() == f.GetType())));
            CopyFieldInfoHelper(OutputSchema, InputSchema);

            // If a selection operator has UnexpandedEntityInequalityConditions set, it is a special
            // selector that requires constructing special logical expressions as FilterExpressions
            // that was not (able to) be created during initial logical plan construction
            //
            // TODO: a better approach is to handle this during plan creation and eliminate having
            //       special logic inside schema propagation logic
            if (UnexpandedEntityInequalityConditions != null)
            {
                // Assert that this operator doesn't carry regular filtering expression, which is currently
                // guaranteed by the Logical Plan creation
                Debug.Assert(FilterExpression == null);

                FilterExpression = UnexpandedEntityInequalityConditions.Aggregate(
                    FilterExpression,
                    (expr, c) =>
                    {
                        var relEnt1 = InputSchema.First(e => e.FieldAlias == c.RelAlias1) as EntityField;
                        var relEnt2 = InputSchema.First(e => e.FieldAlias == c.RelAlias2) as EntityField;
                        Debug.Assert(relEnt1 != null && relEnt2 != null);

                        var condExpr1 = new QueryExpressionBinary()
                        {
                            Operator = OperatorHelper.GetOperator(BinaryOperator.NEQ),
                            LeftExpression = new QueryExpressionProperty()
                            {
                                VariableName = c.RelAlias1,
                                PropertyName = relEnt1.RelSourceJoinField.FieldAlias
                            },
                            RightExpression = new QueryExpressionProperty()
                            {
                                VariableName = c.RelAlias2,
                                PropertyName = relEnt2.RelSourceJoinField.FieldAlias
                            }
                        };
                        var condExpr2 = new QueryExpressionBinary()
                        {
                            Operator = OperatorHelper.GetOperator(BinaryOperator.NEQ),
                            LeftExpression = new QueryExpressionProperty()
                            {
                                VariableName = c.RelAlias1,
                                PropertyName = relEnt1.RelSinkJoinField.FieldAlias
                            },
                            RightExpression = new QueryExpressionProperty()
                            {
                                VariableName = c.RelAlias2,
                                PropertyName = relEnt2.RelSinkJoinField.FieldAlias
                            }
                        };
                        var binExpr = new QueryExpressionBinary()
                        {
                            Operator = OperatorHelper.GetOperator(BinaryOperator.OR),
                            LeftExpression = condExpr1,
                            RightExpression = condExpr2
                        };

                        return expr == null ?
                            binExpr :
                            new QueryExpressionBinary()
                            {
                                Operator = OperatorHelper.GetOperator(BinaryOperator.AND),
                                LeftExpression = expr,
                                RightExpression = binExpr
                            };
                    });
            }

            // Update in-place for property references inside FilterExpression
            if (FilterExpression != null)
            {
                foreach (var prop in FilterExpression.GetChildrenOfType<QueryExpressionProperty>())
                {
                    UpdatePropertyBasedOnAliasFromInputSchema(prop);
                }
            }
        }

        internal override void AppendReferencedProperties(IDictionary<string, EntityField> entityFields)
        {
            // Selection operator may have additional field references in the where/order by conditions that 
            // were not referenced in output schema. we need to add those field references to the list of
            // referenced fields to the input schema
            
            Debug.Assert(FilterExpression != null || OrderByExpressions != null || Limit != null );
            var allPropertyReferences = new List<QueryExpressionProperty>();

            var propsFromFilterExprs = FilterExpression?.GetChildrenQueryExpressionType<QueryExpressionProperty>();
            var propsFromOrderByExprs = OrderByExpressions?.SelectMany(n => n.InnerExpression.GetChildrenQueryExpressionType<QueryExpressionProperty>());

            if(propsFromOrderByExprs != null)
            {
                allPropertyReferences.AddRange(propsFromOrderByExprs);
            }
            if (propsFromFilterExprs != null)
            {
                allPropertyReferences.AddRange(propsFromFilterExprs);
            }

            foreach (var prop in allPropertyReferences)
            {
                var varName = prop.VariableName;
                var fieldName = prop.PropertyName;

                if (prop.Entity != null)
                {
                    // Assert that this is a reference to a whole entity
                    Debug.Assert(fieldName == null);

                    if(!entityFields.ContainsKey(varName))
                    {
                        throw new TranspilerInternalErrorException($"Entity field: '{varName}' does not exist");
                    }

                    var entity = entityFields[varName];

                    // for entity reference, such as MATCH (d:device) RETURN count(d)
                    // we can by default, for node, does count(d.id) instead, and edge does count(d._vertexId) instead
                    if (entity.Type == EntityField.EntityType.Node)
                    {
                        entity.AddReferenceFieldName(entity.NodeJoinField.FieldAlias);
                    }
                    else
                    {
                        entity.AddReferenceFieldName(entity.RelSourceJoinField.FieldAlias);
                    }
                }
                else if (fieldName != null)
                {
                    // field member access (i.e. [VariableName].[fieldName]), just append to list
                    if (!entityFields.ContainsKey(varName))
                    {
                        throw new TranspilerSyntaxErrorException($"Failed to access member '{fieldName}' for '{varName}' which does not exist");
                    }
                    var entity = entityFields[varName];
                    entity.AddReferenceFieldName(fieldName);
                }
                else
                {
                    // reference to an existing field
                    // just check if such field exists or not
                    if (!InputSchema.Any(f2 => f2.FieldAlias == varName))
                    {
                        throw new TranspilerSyntaxErrorException($"'{varName}' does not exist");
                    }
                }
            }
        }
    }
}
