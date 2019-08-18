/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using openCypherTranspiler.Common.GraphSchema;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.openCypherParser.AST;

namespace openCypherTranspiler.LogicalPlanner
{
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
}
