/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using openCypherTranspiler.Common.GraphSchema;
using openCypherTranspiler.Common.Utils;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.openCypherParser.AST;
using openCypherTranspiler.openCypherParser.Common;

namespace openCypherTranspiler.LogicalPlanner
{
    /// <summary>
    /// Logical operator representing the relational algebra to evaluate graph queries
    /// The logical plan is a DAG (directed acyclic graph) of logical operators
    /// This is the base type
    /// </summary>
    public abstract class LogicalOperator
    {
        private List<LogicalOperator> _inOperators = new List<LogicalOperator>();
        private List<LogicalOperator> _outOperators = new List<LogicalOperator>();

        /// <summary>
        /// The schema this operator takes in
        /// </summary>
        /// Note: We thought about making this a getter of fields from output from it's in operator
        ///       however, for certain operators, it is not always a combination of all output, making
        ///       it awkward in some cases to implement field reference propagation. Hence for now, we 
        ///       keep this a separate instanced list to maintain
        public Schema InputSchema { get; set; }

        /// <summary>
        /// The schema this operator outputs
        /// </summary>
        public Schema OutputSchema { get; set; }

        /// <summary>
        /// The level of this particular operator
        /// </summary>
        public abstract int Depth { get; }

        /// <summary>
        /// Debug field: the id of the operator during creation
        /// </summary>
        public int OperatorDebugId { get; set; }

        /// <summary>
        /// Downstream operators
        /// </summary>
        public IEnumerable<LogicalOperator> InOperators
        {
            get
            {
                return _inOperators;
            }
        }

        /// <summary>
        /// Downstream operators
        /// </summary>
        public IEnumerable<LogicalOperator> OutOperators
        {
            get
            {
                return _outOperators;
            }
        }

        internal virtual void AddInOperator(LogicalOperator op)
        {
            if (_inOperators.Contains(op))
            {
                throw new TranspilerInternalErrorException($"Internal error: operator {op} has already been added");
            }
            _inOperators.Add(op);
        }

        internal virtual void AddOutOperator(LogicalOperator op)
        {
            if (_outOperators.Contains(op))
            {
                throw new TranspilerInternalErrorException($"Internal error: operator {op} has already been added");
            }
            _outOperators.Add(op);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine($"{this.GetType().Name}: ");
            sb.AppendLine($"In Schema: {string.Join(", ", InputSchema ?? Enumerable.Empty<Field>())};");
            sb.AppendLine($"Out Schema: {string.Join(", ", OutputSchema ?? Enumerable.Empty<Field>())};");
            return sb.ToString();
        }

        /// <summary>
        /// Operator graph traverse: get all downstream operators of a particular type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IEnumerable<T> GetAllDownstreamOperatorsOfType<T>() where T : LogicalOperator
        {
            var opList = new List<T>();

            if (OutOperators.Count() > 0)
            {
                opList.AddRange(OutOperators.SelectMany(op => op.GetAllDownstreamOperatorsOfType<T>()));
            }

            if (this is T)
            {
                opList.Add(this as T);
            }

            return opList;
        }

        /// <summary>
        /// Operator graph traverse: get all upstream operators of a particular type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IEnumerable<T> GetAllUpstreamOperatorsOfType<T>() where T : LogicalOperator
        {
            var opList = new List<T>();

            if (InOperators.Count() > 0)
            {
                opList.AddRange(
                    InOperators
                    .SelectMany(op => op.GetAllUpstreamOperatorsOfType<T>())
                    .Distinct() // we do have multiplexing (multiple output) for operators in some scenarios, so we need to distinct here
                    );
            }

            if (this is T)
            {
                opList.Add(this as T);
            }

            return opList;
        }


        /// <summary>
        /// Used by logic planner to propagate input schema
        /// </summary>
        internal abstract void PropagateDateTypesForSchema();

        /// <summary>
        /// Used by logic planner to update the list of ReferencedFields
        /// </summary>
        internal virtual void PropagateReferencedPropertiesForEntityFields()
        {
            // lift the referenced fields from the next layer of operators back to this one's output entity fields
            var referredFieldsInDownstreamOps = OutOperators.SelectMany(op => op.InputSchema)
                .Where(f => f is EntityField)
                .Cast<EntityField>()
                .GroupBy(f => f.FieldAlias)
                .ToDictionary(kv => kv.Key, kv => kv.SelectMany(fn => fn.ReferencedFieldAliases).Distinct().ToList());

            foreach (var field in OutputSchema.Where(f => f is EntityField).Cast<EntityField>())
            {
                Debug.Assert(referredFieldsInDownstreamOps.ContainsKey(field.FieldAlias));
                field.AddReferenceFieldNames(referredFieldsInDownstreamOps[field.FieldAlias]);
            }

            // lift the referenced fields in the entity fields from output to input schema of this operator, if
            // applicable
            if ((InputSchema?.Count ?? 0) > 0)
            {
                // handle entity name renamed cases by creating a map from field alias before projection to after 
                // projection
                var aliasMap = new Dictionary<String, String>();

                if (this is ProjectionOperator)
                {
                    var aliasMap1 = (this as ProjectionOperator).ProjectionMap
                        .Where(u => OutputSchema.Where(n => n is EntityField).Any(k => k.FieldAlias == u.Key));
                    aliasMap = aliasMap1?.ToDictionary(n => n.Value.GetChildrenQueryExpressionType<QueryExpressionProperty>().First().VariableName, n => n.Key);
                }
                else
                {
                    // create a dummy alias map ( A -> A, B -> B; not alias name modified) for non-projection operator
                    aliasMap = OutputSchema.Where(n => n is EntityField).ToDictionary(n => n.FieldAlias, n=> n.FieldAlias);

                }

                foreach (var field in InputSchema.Where(f => f is EntityField).Cast<EntityField>())
                {
                    var mappedAlias = (aliasMap.ContainsKey(field.FieldAlias) ?aliasMap[field.FieldAlias]:null);

                    if (mappedAlias != null && referredFieldsInDownstreamOps.ContainsKey(mappedAlias))
                    {
                        field.AddReferenceFieldNames(referredFieldsInDownstreamOps[mappedAlias]);
                    }
                }

                var referredFieldsForUpstream = InputSchema
                    .Where(f => f is EntityField).Cast<EntityField>()
                    .ToDictionary(kv => kv.FieldAlias, kv => kv);

                // Some operators has additional fields may get referenced even they are not in output schema
                // Such as in WHERE or ORDER BY
                // Child logical operator class implement this and does the appending
                AppendReferencedProperties(referredFieldsForUpstream);
            }
        }

        /// <summary>
        /// Incremental addition of reference fields by the logical operator (E.g. WHERE/JOIN can add field references
        /// in addition to those already in the outschema when propagating to inSchema)
        /// To be implemented by each Logical Operator class
        /// </summary>
        /// <param name="entityFields">A list of available entities (with bound fields) in the input for this operator</param>
        internal abstract void AppendReferencedProperties(IDictionary<string, EntityField> entityFields);

        /// <summary>
        /// Helper function to propagate content of fields
        /// </summary>
        /// <param name="targetFields"></param>
        /// <param name="srcFields"></param>
        internal static void CopyFieldInfoHelper(IEnumerable<Field> targetFields, IEnumerable<Field> srcFields)
        {
            // Select operator does not alter schema, so the output should map to input perfectly
            Debug.Assert(targetFields.All(f => srcFields.Any(f2 => f2.FieldAlias == f.FieldAlias && f2.GetType() == f.GetType())));

            var fieldMapping = new Dictionary<Field, Field>();
            foreach (var tarF in targetFields)
            {
                var matchSrcF = srcFields.Where(f => f.FieldAlias == tarF.FieldAlias);
                Debug.Assert(matchSrcF.Count() == 1);
                tarF.Copy(matchSrcF.First());
            }
        }
    }

    /// <summary>
    /// Base type for starting operator in a Logical Plan, equivalent to source tuples in RelationAlgebra
    /// </summary>
    public abstract class StartLogicalOperator : LogicalOperator
    {
        internal override void AddInOperator(LogicalOperator op)
        {
            throw new TranspilerInternalErrorException("StartLogicalOperator derived types does not take any InOperator");
        }

        public override int Depth
        {
            get
            {
                return 0;
            }
        }

        internal override void PropagateDateTypesForSchema()
        {
            // NOOP
            // StartingOperator has no input schema
            // The output schema is populated during Bining at the moment
        }


        internal override void AppendReferencedProperties(IDictionary<string, EntityField> entityFields)
        {
            throw new TranspilerInternalErrorException("DataSourceOperator does not have a input source. Not expected to get called here");
        }
    }

    /// <summary>
    /// Base type for operator that takes just one input
    /// </summary>
    public abstract class UnaryLogicalOperator : LogicalOperator
    {
        public LogicalOperator InOperator
        {
            get
            {
                return InOperators.FirstOrDefault();
            }
        }

        protected void SetInOperator(LogicalOperator op)
        {
            if (InOperators.Count() > 0)
            {
                throw new TranspilerInternalErrorException("InOperator is already set");
            }
            op.AddOutOperator(this);
            AddInOperator(op);
        }

        public override int Depth
        {
            get
            {
                return InOperator.Depth + 1;
            }
        }

        internal abstract void PropagateDateTypesForOutSchema();

        internal override void PropagateDateTypesForSchema()
        {
            // For a unary operator, its input schema contains same set of the fields as the output schema 
            // of its input operator
            var prevOutSchema = InOperator.OutputSchema;

            // Verify and create a map for copying the field metadata, just in case the order changes
            // Any failure in matching the fields are internal bugs somewhere in the initial logical plan
            // creation
            var fieldMapping = new Dictionary<Field, Field>();
            foreach (var inF in InputSchema)
            {
                var matchOutFields = prevOutSchema.Where(f => f.FieldAlias == inF.FieldAlias);
                if (matchOutFields.Count() > 1)
                {
                    throw new TranspilerInternalErrorException($"Ambiguous match of field with alias '{inF.FieldAlias}'");
                }
                if (matchOutFields.Count() == 0)
                {
                    throw new TranspilerInternalErrorException($"Failed to match field with alias '{inF.FieldAlias}'");
                }
                fieldMapping.Add(inF, matchOutFields.First());
            }

            CopyFieldInfoHelper(fieldMapping.Keys, fieldMapping.Values);

            // The output schema varies, depending on each operator's behavior
            // E.g. Selection retains the schema, but Projection may alter it
            PropagateDateTypesForOutSchema();
        }
    }

    /// <summary>
    /// Base type for operator that takes two inputs
    /// </summary>
    public abstract class BinaryLogicalOperator : LogicalOperator
    {
        public LogicalOperator InOperatorLeft
        {
            get
            {
                return InOperators.First();
            }
        }
        public LogicalOperator InOperatorRight
        {
            get
            {
                return InOperators.Last();
            }
        }

        protected void SetInOperators(LogicalOperator opLeft, LogicalOperator opRight)
        {
            if (InOperators.Count() > 0)
            {
                throw new TranspilerInternalErrorException("InOperatorLeft or InOperatorRight is already set.");
            }
            opLeft.AddOutOperator(this);
            opRight.AddOutOperator(this);
            AddInOperator(opLeft);
            AddInOperator(opRight);
        }

        public override int Depth
        {
            get
            {
                return Math.Max(InOperatorLeft.Depth, InOperatorRight.Depth) + 1;
            }
        }

        internal abstract void PropagateDataTypesForInSchema();

        internal virtual void PropagateDataTypesForOutSchema()
        {
            // Default implementation is that output schema doesn't change
            CopyFieldInfoHelper(OutputSchema, InputSchema);
        }

        internal override void PropagateDateTypesForSchema()
        {
            PropagateDataTypesForInSchema();
            PropagateDataTypesForOutSchema();
        }
    }

    /// <summary>
    /// Starting operator representing an entity data source of the graph
    /// </summary>
    public sealed class DataSourceOperator : StartLogicalOperator, IBindable
    {
        public DataSourceOperator(Entity entity)
        {
            Entity = entity;
        }

        public Entity Entity { get; set; }

        /// <summary>
        /// This function bind the data source to a given graph definitions
        /// </summary>
        /// <param name="graphDefinition"></param>
        public void Bind(IGraphSchemaProvider graphDefinition)
        {
            // During binding, we read graph definition of the entity
            // and populate the EntityField object in the output
            // with the list of fields that the node/edge definition can expose
            var properties = new List<ValueField>();
            string entityUniqueName;
            string sourceEntityName = null;
            string sinkEntityName = null;
            ValueField nodeIdField = null;
            ValueField edgeSrcIdField = null;
            ValueField edgeSinkIdField = null;

            try
            {
                if (Entity is NodeEntity)
                {
                    NodeSchema nodeDef = graphDefinition.GetNodeDefinition(Entity.EntityName);
                    entityUniqueName = nodeDef.Id;
                    nodeIdField = new ValueField(nodeDef.NodeIdProperty.PropertyName, nodeDef.NodeIdProperty.DataType);

                    properties.AddRange(nodeDef.Properties.Select(p => new ValueField(p.PropertyName, p.DataType)));
                    properties.Add(nodeIdField);
                }
                else
                {
                    var edgeEnt = Entity as RelationshipEntity;
                    EdgeSchema edgeDef = null;

                    switch (edgeEnt.RelationshipDirection)
                    {
                        case RelationshipEntity.Direction.Forward:
                            edgeDef = graphDefinition.GetEdgeDefinition(edgeEnt.EntityName, edgeEnt.LeftEntityName, edgeEnt.RightEntityName);
                            break;
                        case RelationshipEntity.Direction.Backward:
                            edgeDef = graphDefinition.GetEdgeDefinition(edgeEnt.EntityName, edgeEnt.RightEntityName, edgeEnt.LeftEntityName);
                            break;
                        default:
                            // either direction
                            // TODO: we don't handle 'both' direction yet
                            Debug.Assert(edgeEnt.RelationshipDirection == RelationshipEntity.Direction.Both);
                            edgeDef = graphDefinition.GetEdgeDefinition(edgeEnt.EntityName, edgeEnt.LeftEntityName, edgeEnt.RightEntityName);
                            if (edgeDef == null)
                            {
                                edgeDef = graphDefinition.GetEdgeDefinition(edgeEnt.EntityName, edgeEnt.RightEntityName, edgeEnt.LeftEntityName);
                            }
                            break;
                    }

                    entityUniqueName = edgeDef.Id;
                    sourceEntityName = edgeDef.SourceNodeId;
                    sinkEntityName = edgeDef.SinkNodeId;
                    edgeSrcIdField = new ValueField(edgeDef.SourceIdProperty.PropertyName, edgeDef.SourceIdProperty.DataType);
                    edgeSinkIdField = new ValueField(edgeDef.SinkIdProperty.PropertyName, edgeDef.SinkIdProperty.DataType);

                    properties.AddRange(edgeDef.Properties.Select(p => new ValueField(p.PropertyName, p.DataType)));
                    properties.Add(edgeSrcIdField);
                    properties.Add(edgeSinkIdField);
                }
            }
            catch (KeyNotFoundException e)
            {
                throw new TranspilerBindingException($"Failed to binding entity with alias '{Entity.Alias}' of type '{Entity.EntityName}' to graph definition. Inner error: {e.GetType().Name}: {e.Message}");
            }

            Debug.Assert(OutputSchema.Count == 1
                && OutputSchema.First() is EntityField
                && (OutputSchema.First() as EntityField).EntityName == Entity.EntityName);

            var field = OutputSchema.First() as EntityField;
            field.BoundEntityName = entityUniqueName;
            field.BoundSourceEntityName = sourceEntityName;
            field.BoundSinkEntityName = sinkEntityName;
            field.EncapsulatedFields = properties;
            field.NodeJoinField = nodeIdField;
            field.RelSourceJoinField = edgeSrcIdField;
            field.RelSinkJoinField = edgeSinkIdField;
        }

        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.AppendLine($"DataEntitySource: {Entity};");
            return sb.ToString();
        }
    }

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
                        throw new TranspilerSyntaxErrorException($"Failed to access member '{fieldName} for '{varName}' which does not exist");
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

                    // first of all, bind the type to the variable references
                    foreach (var prop in allPropertyReferences)
                    {
                        var varName = prop.VariableName;
                        var propName = prop.PropertyName;
                        Debug.Assert(prop.VariableName != null);
                        var matchedField = InputSchema.FirstOrDefault(f => f.FieldAlias == varName);

                        if (matchedField == null)
                        {
                            throw new TranspilerBindingException($"Failed to find input matching field alias {varName}");
                        }

                        if (string.IsNullOrEmpty(propName))
                        {
                            // direct reference to an alias (value column or entity column)
                            if (matchedField is ValueField)
                            {
                                prop.DataType = (matchedField as ValueField).FieldType;
                            }
                            else
                            {
                                // entity field reference in a single field expression
                                // this is valid only in handful situations, such as Count(d), Count(distinct(d))
                                // in such case, we populate the Entity object with correct entity type so that code generator can use it later
                                Debug.Assert(matchedField is EntityField);
                                var matchedEntity = matchedField as EntityField;
                                prop.Entity = matchedEntity.Type == EntityField.EntityType.Node ?
                                    new NodeEntity() { EntityName = matchedEntity.EntityName, Alias = matchedEntity.FieldAlias } as Entity:
                                    new RelationshipEntity() { EntityName = matchedEntity.EntityName, Alias = matchedEntity.FieldAlias } as Entity;
                            }
                        }
                        else
                        {
                            // property dereference of an entity column
                            if (!(matchedField is EntityField))
                            {
                                throw new TranspilerBindingException($"Failed to dereference property {propName} for alias {varName}, which is not an alias of entity type as expected");
                            }
                            var entField = matchedField as EntityField;
                            var entPropField = entField.EncapsulatedFields.FirstOrDefault(f => f.FieldAlias == propName);
                            if (entPropField == null)
                            {
                                throw new TranspilerBindingException($"Failed to dereference property {propName} for alias {varName}, Entity type {entField.BoundEntityName} does not have a property named {propName}");
                            }
                            entField.AddReferenceFieldName(propName);
                            prop.DataType = entPropField.FieldType;
                        }
                    }

                    // do data type evaluation
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

    /// <summary>
    /// Operator to satisfy UNION
    /// </summary>
    public sealed class SetOperator : BinaryLogicalOperator
    {
        public SetOperator(LogicalOperator leftOp, LogicalOperator rightOp, SetOperationType setOpType)
        {
            SetOperation = setOpType;
            SetInOperators(leftOp, rightOp);
        }

        public enum SetOperationType
        {
            Union,
            UnionAll,
        }

        public SetOperationType SetOperation { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.AppendLine($"Set Operation Type: {SetOperation};");
            return sb.ToString();
        }

        internal override void PropagateDataTypesForInSchema()
        {
            // For SetOperator, out schema should be the same for union from 2 source of input

            var leftSchema = InOperatorLeft.OutputSchema;
            var rightSchema = InOperatorRight.OutputSchema;

            if (leftSchema.Count != rightSchema.Count)
            {
                throw new TranspilerSyntaxErrorException("All sub queries in an UNION must have the same column names");
            }

            // validate left schema and see if it is compatible with right schema
            foreach (var z in leftSchema.Zip(rightSchema, (l, r) => (LeftField: l, RightField: r)))
            {
                if (z.LeftField.FieldAlias != z.RightField.FieldAlias)
                {
                    throw new TranspilerSyntaxErrorException($"Column name mismatch: left='{z.LeftField.FieldAlias}', right='{z.LeftField.FieldAlias}'");
                }

                if (z.LeftField.GetType() != z.RightField.GetType())
                {
                    throw new TranspilerSyntaxErrorException($"Column type mismatch for column '{z.LeftField.FieldAlias}'");
                }

                if (z.LeftField is EntityField)
                {
                    var l = z.LeftField as EntityField;
                    var r = z.RightField as EntityField;

                    if (l.BoundEntityName != r.BoundEntityName)
                    {
                        throw new TranspilerSyntaxErrorException($"Column type mismatch for column '{z.LeftField.FieldAlias}': left='{l.BoundEntityName}', right='{r.BoundEntityName}'");
                    }
                }
                else
                {
                    Debug.Assert(z.LeftField is ValueField && z.RightField is ValueField);
                    var l = z.LeftField as ValueField;
                    var r = z.RightField as ValueField;
                    if (l.FieldType != r.FieldType)
                    {
                        throw new TranspilerSyntaxErrorException($"Column type mismatch for column '{z.LeftField.FieldAlias}': left='{l.FieldType}', right='{r.FieldType}'");
                    }
                }
            }

            // Snap to the column names, type and order of the later sub-query
            CopyFieldInfoHelper(OutputSchema, rightSchema);
        }

        internal override void AppendReferencedProperties(IDictionary<string, EntityField> entityFields)
        {
            // NOOP. Set operators does not have addition referenced properties than those already on the output
        }
    }

    /// <summary>
    /// Operator to satisfy JOIN
    /// </summary>
    public sealed class JoinOperator : BinaryLogicalOperator
    {
        /// <summary>
        /// Keeps track of what entities are joint together
        /// </summary>
        private List<JoinKeyPair> _joinPairs = new List<JoinKeyPair>();

        /// <summary>
        /// Structure designate how two entity instance should be joined together
        /// </summary>
        public class JoinKeyPair
        {
            public enum JoinKeyPairType
            {
                None,
                Source, // Node join to Relationship's SourceId
                Sink,   // Node join to Relationship's SinkId
                Either, // Node allowed to join either Source or Sink, whichever is applicable
                Both,   // Node join to both source and sink
                NodeId, // Node to node join
            }

            public string NodeAlias { get; set; }
            public string RelationshipOrNodeAlias { get; set; }
            public JoinKeyPairType Type { get; set; }

            #region Equality comparison overrides
            public override bool Equals(object obj)
            {
                var compObj = obj as JoinKeyPair;
                return compObj?.Type == Type &&
                    compObj?.NodeAlias == NodeAlias &&
                    compObj?.RelationshipOrNodeAlias == RelationshipOrNodeAlias;
            }

            public override int GetHashCode()
            {
                return FNVHash.GetFNVHash(Type, NodeAlias, RelationshipOrNodeAlias);
            }

            public static bool operator ==(JoinKeyPair lhs, JoinKeyPair rhs) => lhs.Equals(rhs);
            public static bool operator !=(JoinKeyPair lhs, JoinKeyPair rhs) => !lhs.Equals(rhs);
            
            #endregion Equality comparison overrides

            public override string ToString()
            {
                return $"JoinPair: Node={NodeAlias} RelOrNode={RelationshipOrNodeAlias} Type=({Type})";
            }

        }

        /// <summary>
        /// Type of join
        /// </summary>
        public enum JoinType
        {
            // ordered from least constrained to most
            Cross = 0,
            Left = 1,
            Inner = 2,
        }

        public JoinOperator(LogicalOperator leftOp, LogicalOperator rightOp, JoinType joinType)
        {
            Type = joinType;
            SetInOperators(leftOp, rightOp);
        }

        /// <summary>
        /// Type of join to be performed for this JOIN operator for all the join key pairs
        /// </summary>
        public JoinType Type { get; set; }

        /// <summary>
        /// List of joins needed by this operator between entities from the input
        /// </summary>
        public IReadOnlyCollection<JoinKeyPair> JoinPairs { get { return _joinPairs.AsReadOnly(); } }

        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.AppendLine($"Join Type: {Type};");
            sb.AppendLine($"Join Operations: {string.Join(", ", JoinPairs.Select(kv => $"{kv.NodeAlias} |><|_{kv.Type.ToString().First()} {kv.RelationshipOrNodeAlias}"))};");
            return sb.ToString();
        }

        internal override void PropagateDataTypesForInSchema()
        {
            // Input for JOIN is the combination of both sources

            var prevLeftSchema = InOperatorLeft.OutputSchema;
            var prevRightSchema = InOperatorRight.OutputSchema;
            var prevOutFields = prevLeftSchema.Union(prevRightSchema);
            var fieldMapping = new Dictionary<Field, Field>();

            foreach (var inF in InputSchema)
            {
                var matchOutFields = prevOutFields.Where(f => f.FieldAlias == inF.FieldAlias);
                if (matchOutFields.Count() > 1)
                {
                    // In case of join by node ids between left and right input (such as in MATCH .. OPTIONAL MATCH ... WHERE case)
                    // we will take the from left side and ignore duplications
                    // otherwise throws exception
                    if (!JoinPairs.All(jp => jp.Type == JoinKeyPair.JoinKeyPairType.NodeId))
                    {
                        throw new TranspilerInternalErrorException($"Ambiguous match of field with alias '{inF.FieldAlias}'");
                    }
                }
                if (matchOutFields.Count() == 0)
                {
                    throw new TranspilerInternalErrorException($"Failed to match field with alias '{inF.FieldAlias}'");
                }
                fieldMapping.Add(inF, matchOutFields.First());
            }

            CopyFieldInfoHelper(InputSchema, fieldMapping.Values);

            // additional validation on if relationships can satisfy the joins if directional traversal is explicitly specified
            foreach (var joinPair in JoinPairs)
            {
                var nodeEntity = InputSchema.First(s => s.FieldAlias == joinPair.NodeAlias) as EntityField;
                var relEntity = InputSchema.First(s => s.FieldAlias == joinPair.RelationshipOrNodeAlias) as EntityField;
                switch (joinPair.Type)
                {
                    case JoinKeyPair.JoinKeyPairType.Source:
                        if (relEntity.BoundSourceEntityName != nodeEntity.BoundEntityName)
                        {
                            throw new TranspilerBindingException($"Cannot find relationship ({nodeEntity.BoundEntityName})-[{relEntity.BoundSourceEntityName}]->(*), for {joinPair.NodeAlias} and {joinPair.RelationshipOrNodeAlias}");
                        }
                        break;
                    case JoinKeyPair.JoinKeyPairType.Sink:
                        if (relEntity.BoundSinkEntityName != nodeEntity.BoundEntityName)
                        {
                            throw new TranspilerBindingException($"Cannot find relationship ({nodeEntity.BoundEntityName})<-[{relEntity.BoundSourceEntityName}]-(*), for {joinPair.NodeAlias} and {joinPair.RelationshipOrNodeAlias}");
                        }
                        break;
                    default:
                        // in .Either, or .Both case
                        // no validation need to be done as Binding should already enforce the existence of the relationships
                        break;
                }
            }
        }

        internal override void PropagateDataTypesForOutSchema()
        {
            // Output for JOIN depends on type of the join
            // For LEFT join, attributes becomes non-nullable
            // For CROSS join or INNER join, attributes type remains unchanged after join
            if (Type == JoinType.Left)
            {
                var prevLeftSchema = InOperatorLeft.OutputSchema;
                var prevRightSchema = InOperatorRight.OutputSchema;

                var fieldMapping = new Dictionary<Field, Field>();
                foreach (var outField in OutputSchema)
                {
                    var inFieldMatches = InputSchema.Where(f => f.FieldAlias == outField.FieldAlias);
                    Debug.Assert(InputSchema.Where(f => f.FieldAlias == outField.FieldAlias).Count() == 1); // must have match in IN for any OUT field
                    var inField = inFieldMatches.First();
                    // we made it that left schema LEFT OUTTER JOIN with right schema always
                    var isFromRight = !prevLeftSchema.Any(f => f.FieldAlias == inField.FieldAlias);

                    // copy over the schema first
                    outField.Copy(inField);

                    // make adjustment for outter join
                    if (inField is ValueField)
                    {
                        Debug.Assert(outField.GetType() == inField.GetType()); // join doesn't alter field types
                        var outFieldSingleField = outField as ValueField;

                        if (isFromRight && !TypeHelper.CanAssignNullToType(outFieldSingleField.FieldType))
                        {
                            // make it nullable variant of the original type
                            outFieldSingleField.FieldType = TypeHelper.GetNullableTypeForType(outFieldSingleField.FieldType);
                        }
                    }
                    else
                    {
                        Debug.Assert(outField is EntityField);
                        var outEntCapFields = (outField as EntityField).EncapsulatedFields;

                        if (isFromRight)
                        {
                            foreach (var outEntCapField in outEntCapFields)
                            {
                                if (!TypeHelper.CanAssignNullToType(outEntCapField.FieldType))
                                {
                                    // make it nullable variant of the original type
                                    outEntCapField.FieldType = TypeHelper.GetNullableTypeForType(outEntCapField.FieldType);
                                }
                            }
                        }
                    }
                }

            }
            else
            {
                base.PropagateDataTypesForOutSchema();
            }
        }

        internal override void AppendReferencedProperties(IDictionary<string, EntityField> entityFields)
        {
            // NOOP. Besides join keys, JOIN operator does not introduce any additionally referenced fields
            // The handling of join key is left for later stage
        }

        /// <summary>
        /// For adding to the maintained list of what entities are joint together by this operator
        /// </summary>
        /// <param name="entityAlias1">the alias on the left side of the join</param>
        /// <param name="entityAlias2">the alias on the right side of the join</param>
        internal void AddJoinPair(JoinKeyPair joinKeyPair)
        {
            Debug.Assert(InputSchema.Where(f => f is EntityField && f.FieldAlias == joinKeyPair.NodeAlias).Count() == 1);
            Debug.Assert(InputSchema.Where(f => f is EntityField && f.FieldAlias == joinKeyPair.RelationshipOrNodeAlias).Count() == 1);
            _joinPairs.Add(joinKeyPair);
        }
    }
}
