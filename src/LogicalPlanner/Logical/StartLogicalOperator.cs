/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using openCypherTranspiler.Common.Exceptions;

namespace openCypherTranspiler.LogicalPlanner
{
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
}
