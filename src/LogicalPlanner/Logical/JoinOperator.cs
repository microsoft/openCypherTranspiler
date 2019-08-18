/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using openCypherTranspiler.Common.Utils;
using openCypherTranspiler.Common.Exceptions;

namespace openCypherTranspiler.LogicalPlanner
{
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
