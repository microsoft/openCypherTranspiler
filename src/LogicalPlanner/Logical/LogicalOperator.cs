/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.openCypherParser.AST;

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

        internal void UpdatePropertyBasedOnAliasFromInputSchema(QueryExpressionProperty prop)
        {
            var matchedField = InputSchema.FirstOrDefault(f => f.FieldAlias == prop.VariableName);

            if (matchedField == null)
            {
                throw new TranspilerBindingException($"Alias '{prop.VariableName}' does not exist in the current context");
            }

            if (string.IsNullOrEmpty(prop.PropertyName))
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
                        new NodeEntity() { EntityName = matchedEntity.EntityName, Alias = matchedEntity.FieldAlias } as Entity :
                        new RelationshipEntity() { EntityName = matchedEntity.EntityName, Alias = matchedEntity.FieldAlias } as Entity;
                }
            }
            else
            {
                // property dereference of an entity column
                if (!(matchedField is EntityField))
                {
                    throw new TranspilerBindingException($"Failed to dereference property {prop.PropertyName} for alias {prop.VariableName}, which is not an alias of entity type as expected");
                }
                var entField = matchedField as EntityField;
                var entPropField = entField.EncapsulatedFields.FirstOrDefault(f => f.FieldAlias == prop.PropertyName);
                if (entPropField == null)
                {
                    throw new TranspilerBindingException($"Failed to dereference property {prop.PropertyName} for alias {prop.VariableName}, Entity type {entField.BoundEntityName} does not have a property named {prop.PropertyName}");
                }
                entField.AddReferenceFieldName(prop.PropertyName);
                prop.DataType = entPropField.FieldType;
            }
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
}
