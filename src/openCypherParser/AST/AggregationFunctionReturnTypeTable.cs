/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using openCypherTranspiler.openCypherParser.Common;

namespace openCypherTranspiler.openCypherParser.AST
{
    public static class AggregationFunctionReturnTypeTable
    {
        public static readonly IDictionary<(AggregationFunction, Type), Type> TypeMapTable = new Dictionary<(AggregationFunction, Type), Type>()
        {
            { (AggregationFunction.Count, Type.GetType("System.Int32")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.Double")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.Int64")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.Int16")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.UInt16")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.UInt64")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.Byte")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.Single")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.UInt32")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.SByte")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.Decimal")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.String")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.Boolean")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.DateTime")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.Byte[]")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, Type.GetType("System.Object")), Type.GetType("System.Int64")},
            { (AggregationFunction.Count, default(Type)), Type.GetType("System.Int64")},

            { (AggregationFunction.Avg, Type.GetType("System.Int32")), Type.GetType("System.Nullable`1[System.Double]")},
            { (AggregationFunction.Avg, Type.GetType("System.Double")), Type.GetType("System.Nullable`1[System.Double]")},
            { (AggregationFunction.Avg, Type.GetType("System.Int64")), Type.GetType("System.Nullable`1[System.Double]")},
            { (AggregationFunction.Avg, Type.GetType("System.Int16")), Type.GetType("System.Nullable`1[System.Double]")},
            { (AggregationFunction.Avg, Type.GetType("System.UInt16")), Type.GetType("System.Nullable`1[System.Double]")},
            { (AggregationFunction.Avg, Type.GetType("System.UInt64")), Type.GetType("System.Nullable`1[System.Double]")},
            { (AggregationFunction.Avg, Type.GetType("System.Byte")), Type.GetType("System.Nullable`1[System.Double]")},
            { (AggregationFunction.Avg, Type.GetType("System.Single")), Type.GetType("System.Nullable`1[System.Double]")},
            { (AggregationFunction.Avg, Type.GetType("System.UInt32")), Type.GetType("System.Nullable`1[System.Double]")},
            { (AggregationFunction.Avg, Type.GetType("System.SByte")), Type.GetType("System.Nullable`1[System.Double]")},
            { (AggregationFunction.Avg, Type.GetType("System.Decimal")), Type.GetType("System.Nullable`1[System.Decimal]")},

            { (AggregationFunction.Sum, Type.GetType("System.Int32")),Type.GetType("System.Int64") },
            { (AggregationFunction.Sum, Type.GetType("System.Double")),Type.GetType("System.Double")},
            { (AggregationFunction.Sum, Type.GetType("System.Int64")),Type.GetType("System.Int64") },
            { (AggregationFunction.Sum, Type.GetType("System.Int16")),Type.GetType("System.Int64") },
            { (AggregationFunction.Sum, Type.GetType("System.UInt16")),Type.GetType("System.Int64") },
            { (AggregationFunction.Sum, Type.GetType("System.UInt64")),Type.GetType("System.Double")},
            { (AggregationFunction.Sum, Type.GetType("System.Byte")),Type.GetType("System.Int64") },
            { (AggregationFunction.Sum, Type.GetType("System.Single")),Type.GetType("System.Double")},
            { (AggregationFunction.Sum, Type.GetType("System.UInt32")),Type.GetType("System.Int64") },
            { (AggregationFunction.Sum, Type.GetType("System.SByte")),Type.GetType("System.Int64") },
            { (AggregationFunction.Sum, Type.GetType("System.Decimal")),Type.GetType("System.Decimal")},

        };
    }
}
