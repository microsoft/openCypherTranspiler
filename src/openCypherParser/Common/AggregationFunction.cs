/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

namespace openCypherTranspiler.openCypherParser.Common
{
    /// <summary>
    /// Enum type for aggregation functions supported by this code generator
    /// </summary>
    public enum AggregationFunction
    {
        Invalid,
        None,
        Sum,
        Avg,
        Count,
        Max,
        Min,
        First,
        Last,
        PercentileCont,
        PercentileDisc,
        StDev,
        StDevP
    }

    class AggregationFunctionHelper
    {
        /// <summary>
        /// Parse a function into aggregation function enum
        /// </summary>
        /// <param name="functionName"></param>
        /// <param name="aggFunc"></param>
        /// <returns></returns>
        public static bool TryParse(string functionName, out AggregationFunction aggFunc)
        {
            string lowerCaseFunctionName = functionName.ToLower();

            switch (lowerCaseFunctionName)
            {
                case "avg":
                    aggFunc = AggregationFunction.Avg;
                    break;
                case "sum":
                    aggFunc = AggregationFunction.Sum;
                    break;
                case "count":
                    aggFunc = AggregationFunction.Count;
                    break;
                case "max":
                    aggFunc = AggregationFunction.Max;
                    break;
                case "min":
                    aggFunc = AggregationFunction.Min;
                    break;
                case "first":
                    // not supported by cypher but cosmos
                    aggFunc = AggregationFunction.First;
                    break;
                case "last":
                    // not supported by cypher but cosmos
                    aggFunc = AggregationFunction.Last;
                    break;
                case "percentilecont":
                    aggFunc = AggregationFunction.PercentileCont;
                    break;
                case "percentiledisc":
                    aggFunc = AggregationFunction.PercentileDisc;
                    break;
                case "stdev":
                    aggFunc = AggregationFunction.StDev;
                    break;
                case "stdevp":
                    aggFunc = AggregationFunction.StDevP;
                    break;
                default:
                    // treat it as non-aggregating functions
                    aggFunc = AggregationFunction.Invalid;
                    return false;
            }

            // indicating it is an aggregating function
            return true;
        }
    }
}
