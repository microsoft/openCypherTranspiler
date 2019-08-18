/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Linq;
using openCypherTranspiler.Common.Exceptions;

namespace openCypherTranspiler.LogicalPlanner
{
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
}
