/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Linq;
using openCypherTranspiler.Common.Exceptions;

namespace openCypherTranspiler.LogicalPlanner
{
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
}
