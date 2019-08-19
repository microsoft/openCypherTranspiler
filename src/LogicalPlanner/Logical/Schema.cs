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
    /// represents a single alias (could be a value column or an entity) in a schema
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
    /// A field which is an entity (a set of value fields derived from node or relationship entities)
    /// </summary>
    public class EntityField : Field
    {
        private ISet<string> _referencedFieldNames = new HashSet<string>();

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
            this.NodeJoinField = fieldSrc.NodeJoinField?.Clone() as ValueField;
            this.RelSinkJoinField = fieldSrc.RelSinkJoinField?.Clone() as ValueField;
            this.RelSourceJoinField = fieldSrc.RelSourceJoinField?.Clone() as ValueField;
            this.EncapsulatedFields = fieldSrc.EncapsulatedFields?.Select(f => f.Clone() as ValueField).ToList();
            _referencedFieldNames.Clear();
            this.AddReferenceFieldNames(fieldSrc.ReferencedFieldAliases?.ToList());
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
        /// List of fields that this entity can expand into
        /// </summary>
        public IList<ValueField> EncapsulatedFields { get; set; }

        /// <summary>
        /// Used for keep tracking what fields of this entity were actually accessed
        /// </summary>
        public IReadOnlyCollection<string> ReferencedFieldAliases { get { return _referencedFieldNames.ToList().AsReadOnly(); } }

        /// <summary>
        /// The field name for the node's join key
        /// </summary>
        public ValueField NodeJoinField { get; set; }

        /// <summary>
        /// The field name for the edge's source join key
        /// </summary>
        public ValueField RelSourceJoinField { get; set; }

        /// <summary>
        /// The field name for the relationship's sink join key
        /// </summary>
        public ValueField RelSinkJoinField { get; set; }


        public override string ToString()
        {
            return $"{Type}Field: {FieldAlias}({EntityName})";
        }

        public override Field Clone()
        {
            return new EntityField(this);
        }

        public void AddReferenceFieldName(string fieldName)
        {
            if (!EncapsulatedFields.Any(n => n.FieldAlias == fieldName))
            {
                throw new TranspilerSyntaxErrorException($"Entity '{EntityName}' does not have a field named '{fieldName}'.");
            }
            
            if (!_referencedFieldNames.Contains(fieldName))
            {
                _referencedFieldNames.Add(fieldName);
            }
        }

        public void AddReferenceFieldNames(IEnumerable<string> fieldNames)
        {
            fieldNames.ToList().ForEach(a => AddReferenceFieldName(a));
        }

    }

    /// <summary>
    /// A field which is a scalar value column
    /// </summary>
    public class ValueField : Field
    {
        public ValueField(string alias) : base(alias) { }

        /// <summary>
        /// Copy constructor
        /// </summary>
        /// <param name="field"></param>
        public ValueField(ValueField field) : base(field.FieldAlias)
        {
            Copy(field);
        }

        public ValueField(string alias, Type fieldType) : base(alias)
        {
            FieldType = fieldType;
        }

        public override void Copy(Field field)
        {
            var fieldSrc = field as ValueField;
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
            return new ValueField(this);
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
