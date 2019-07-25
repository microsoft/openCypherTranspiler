/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using openCypherTranspiler.Common.Exceptions;

namespace openCypherTranspiler.LogicalPlanner
{
    /// <summary>
    /// a field of a schema
    /// </summary>
    public abstract class Field
    {
        public Field(string alias)
        {
            FieldAlias = alias;
        }

        /// <summary>
        /// The local unique name (in current operator's context) to refer this field
        /// </summary>
        public string FieldAlias { get; set; }

        /// <summary>
        /// Create a deep copy of this Field object
        /// </summary>
        /// <returns></returns>
        public abstract Field Clone();

        /// <summary>
        /// Copy the information from another field to itself (except for alias)
        /// </summary>
        public abstract void Copy(Field f);
    }

    /// <summary>
    /// A field which is an entity (a set of columns forming a node or edge stream/stream group)
    /// </summary>
    public class EntityField : Field
    {
        private ISet<string> _referencedAliases = new HashSet<string>();

        public EntityField(string alias, string entityName, EntityType type) : base(alias)
        {
            EntityName = entityName;
            Type = type;
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        /// <param name="field"></param>
        public EntityField(EntityField field) : base(field.FieldAlias)
        {
            Copy(field);
        }

        public override void Copy(Field field)
        {
            var fieldSrc = field as EntityField;
            Debug.Assert(fieldSrc != null);
            this.EntityName = fieldSrc.EntityName;
            this.Type = fieldSrc.Type;
            this.BoundEntityName = fieldSrc.BoundEntityName;
            this.BoundSourceEntityName = fieldSrc.BoundSourceEntityName;
            this.BoundSinkEntityName = fieldSrc.BoundSinkEntityName;
            this.NodeJoinField = fieldSrc.NodeJoinField?.Clone() as SingleField;
            this.RelSinkJoinField = fieldSrc.RelSinkJoinField?.Clone() as SingleField;
            this.RelSourceJoinField = fieldSrc.RelSourceJoinField?.Clone() as SingleField;
            this.EncapsulatedFields = fieldSrc.EncapsulatedFields?.Select(f => f.Clone() as SingleField).ToList();
            _referencedAliases.Clear();
            this.AddReferenceFieldAliases(fieldSrc.ReferencedFieldAliases?.ToList());
        }

        public enum EntityType
        {
            Node,
            Relationship
        }

        /// <summary>
        /// For node this is node name, for edge this is the edge verb
        /// </summary>
        public string EntityName { get; set; }

        /// <summary>
        /// This is the name of the entity that this is eventually bound to in the graph definition
        /// For node, it is usually same as EntityName, but for edge, it is the unique entity name
        /// from the graph definition which is in the format of node_in-verb-node_out
        /// </summary>
        public string BoundEntityName { get; set; }

        /// <summary>
        /// This is the name of the source entity name for this relationship (in the graph definition)
        /// Only non null for edge type of entity
        /// </summary>
        public string BoundSourceEntityName { get; set; }

        /// <summary>
        /// This is the name of the sink entity name for this relationship (in the graph definition)
        /// Only non null for edge type of entity
        /// </summary>
        public string BoundSinkEntityName { get; set; }

        /// <summary>
        /// If this entity is node or edge
        /// </summary>
        public EntityType Type { get; set; }

        /// <summary>
        /// List of fields that this entity can expand into (namely, property reference) 
        /// </summary>
        public IList<SingleField> EncapsulatedFields { get; set; }

        /// <summary>
        /// Used for keep tracking what properties of this entity were actually accessed
        /// </summary>
        public IReadOnlyCollection<string> ReferencedFieldAliases { get { return _referencedAliases.ToList().AsReadOnly(); } }

        /// <summary>
        /// The field name for the node's join key
        /// </summary>
        public SingleField NodeJoinField { get; set; }

        /// <summary>
        /// The field name for the edge's source join key
        /// </summary>
        public SingleField RelSourceJoinField { get; set; }

        /// <summary>
        /// The field name for the relationship's sink join key
        /// </summary>
        public SingleField RelSinkJoinField { get; set; }


        public override string ToString()
        {
            return $"{Type}Field: {FieldAlias}({EntityName})";
        }

        public override Field Clone()
        {
            return new EntityField(this);
        }

        public void AddReferenceFieldAlias(string fieldAlias)
        {
            if(EncapsulatedFields.All(n => n.FieldAlias != fieldAlias))
            {
                throw new TranspilerSyntaxErrorException($"field alias {fieldAlias} not existed in entity:{EntityName}.");
            }
            else if(!_referencedAliases.Contains(fieldAlias))
            {
                _referencedAliases.Add(fieldAlias);
            }
        }

        public void AddReferenceFieldAliases(IEnumerable<string> fieldAliases)
        {
            fieldAliases.ToList().ForEach(a => AddReferenceFieldAlias(a));
        }

    }

    /// <summary>
    /// A field which is a column
    /// </summary>
    public class SingleField : Field
    {
        public SingleField(string alias) : base(alias) { }

        /// <summary>
        /// Copy constructor
        /// </summary>
        /// <param name="field"></param>
        public SingleField(SingleField field) : base(field.FieldAlias)
        {
            Copy(field);
        }

        public SingleField(string alias, Type fieldType) : base(alias)
        {
            FieldType = fieldType;
        }

        public override void Copy(Field field)
        {
            var fieldSrc = field as SingleField;
            Debug.Assert(fieldSrc != null);
            this.FieldType = fieldSrc.FieldType;
        }

        public Type FieldType { get; set; }


        public override string ToString()
        {
            return $"Field: {FieldAlias}{(FieldType == default(Type) ? "(?)" : $"({FieldType.Name})")}";
        }

        public override Field Clone()
        {
            return new SingleField(this);
        }
    }


    /// <summary>
    /// a collection of fields represent the schema of input or output of a logical operator
    /// </summary>
    public class Schema : List<Field>
    {
        public Schema()
        {
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        /// <param name="field"></param>
        public Schema(Schema schema)
        {
            AddRange(schema.Select(f => f.Clone()).ToList());
        }

        public Schema(IEnumerable<Field> fields)
        {
            AddRange(fields.ToList());
        }

    }
}
