/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using openCypherTranspiler.Common.Exceptions;

namespace openCypherTranspiler.LogicalPlanner
{
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
}
