/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace openCypherTranspiler.SQLRenderer.Test
{
    public class DataTableComparisonHelper
    {
        /// <summary>
        /// Since Neo4j result is not strictly typed, hence we have an approximate comparer to match up columns
        /// </summary>
        class ApproximateColumnComparer : IEqualityComparer<Tuple<string, Type>>
        {
            public static Dictionary<Type, Type> EqualityGroups = new Dictionary<Type, Type>()
            {
                { typeof(bool), typeof(bool) },
                { typeof(int), typeof(Int64) },
                { typeof(Int16), typeof(Int64) },
                { typeof(Int64), typeof(Int64) },
                { typeof(uint), typeof(Int64) },
                { typeof(UInt16), typeof(Int64) },
                { typeof(UInt64), typeof(Int64) },
                { typeof(string), typeof(string) },
                { typeof(float), typeof(double) },
                { typeof(double), typeof(double) },
                { typeof(object), typeof(object) },
            };

            public bool Equals(Tuple<string, Type> x, Tuple<string, Type> y)
            {
                return x.Item1 == y.Item1 && EqualityGroups[x.Item2] == EqualityGroups[y.Item2];
            }

            public int GetHashCode(Tuple<string, Type> obj)
            {
                return obj.Item1.GetHashCode() ^ EqualityGroups[obj.Item2].GetHashCode();
            }
        }

        private bool ApproximateValueComparer(object x, object y)
        {
            var eqTypes = ApproximateColumnComparer.EqualityGroups;

            if (x == DBNull.Value || y == DBNull.Value)
            {
                // special case for string as our test data creation tool cannot handle
                // "" vs null for string case
                if (x.GetType() == typeof(string) || y.GetType() == typeof(string))
                {
                    var isXEmptyOrNullStr = x == DBNull.Value ? true : string.IsNullOrEmpty((string)x);
                    var isYEmptyOrNullStr = y == DBNull.Value ? true : string.IsNullOrEmpty((string)y);
                    return isXEmptyOrNullStr == isYEmptyOrNullStr;
                }

                return x == DBNull.Value && y == DBNull.Value;
            }

            Assert.IsTrue(eqTypes[x.GetType()] == eqTypes[y.GetType()]);
            var tarType = eqTypes[x.GetType()];

            if (tarType == typeof(Int64))
            {
                var x_c = Convert.ToInt64(x);
                var y_c = Convert.ToInt64(y);
                return x_c == y_c;
            }
            else if (tarType == typeof(bool))
            {
                var x_c = Convert.ToBoolean(x);
                var y_c = Convert.ToBoolean(y);
                return x_c == y_c;
            }
            else if (tarType == typeof(double))
            {
                var x_c = Convert.ToDouble(x);
                var y_c = Convert.ToDouble(y);
                return Math.Abs(x_c - y_c) / Math.Max(Math.Abs(x_c), Math.Abs(y_c)) < 0.0001;
            }
            else if (tarType == typeof(string))
            {
                var x_c = x is string ? (string)x : x.ToString();
                var y_c = y is string ? (string)y : y.ToString();
                return x_c.Equals(y_c);
            }
            else
            {
                return x.Equals(y);
            }
        }

        public void CompareDataTables(DataTable leftTable, DataTable rightTable, bool compareOrder = false)
        {
            // Shallow test to ensure cardinatlity is the same at least
            Assert.IsTrue(leftTable.Columns.Count == rightTable.Columns.Count && leftTable.Rows.Count == rightTable.Rows.Count);

            // Neo4j returns no data type at all if no result, if it happens, only compare record count
            if (leftTable.Rows.Count > 0)
            {
                // Schema test
                var leftSchema = leftTable.Columns.Cast<DataColumn>().Select(c => new Tuple<string, Type>(c.ColumnName, c.DataType)).OrderBy(n => n.Item1).ToList();
                var rightSchema = rightTable.Columns.Cast<DataColumn>().Select(c => new Tuple<string, Type>(c.ColumnName, c.DataType)).OrderBy(n => n.Item1).ToList();
                Assert.IsTrue(leftSchema.SequenceEqual(rightSchema, new ApproximateColumnComparer()));

                // Row by row comparison
                if (compareOrder)
                {
                    throw new NotImplementedException("Ordered comparison is not implemented");
                }
                else
                {
                    var leftSchemaColumns = leftSchema.Select(c => c.Item1).ToList();
                    var orderBy = string.Join(", ", leftSchemaColumns);
                    DataView leftSorted = new DataView(leftTable);
                    leftSorted.Sort = orderBy;
                    DataView rightSorted = new DataView(rightTable);
                    rightSorted.Sort = orderBy;

                    // change the order of columns
                    leftSorted.SetColumnsOrder(leftSchemaColumns);
                    rightSorted.SetColumnsOrder(leftSchemaColumns);

                    for (int i = 0; i < leftSorted.Count; i++)
                    {
                        var leftRow = leftSorted[i];
                        var rightRow = rightSorted[i];
                        for (int j = 0; j < leftSchema.Count; j++)
                        {
                            var leftVal = leftRow[j];
                            var rightVal = rightRow[j];
                            if (leftVal == null || rightVal == null)
                            {
                                Assert.IsTrue(leftVal == rightVal);
                            }
                            else
                            {
                                var isValueSimilar = ApproximateValueComparer(leftRow[j], rightRow[j]);
                                if (!isValueSimilar)
                                {
                                    Debug.WriteLine($"Comparison failed for row: {i} col {j}({leftSchema[j].Item1}):");
                                    Debug.WriteLine($"   rightVal: {rightRow[j]}, leftVal: {leftRow[j]}");
                                }
                                Assert.IsTrue(isValueSimilar);
                            }
                        }
                    }
                }
            }
            else
            {
                Assert.IsTrue(rightTable.Rows.Count == 0);
            }


        }

    }



    public static class DataViewExtensions
    {
        public static void SetColumnsOrder(this DataView view, IList<string> columnNames)
        {
            int columnIndex = 0;
            foreach (var columnName in columnNames)
            {
                view.Table.Columns[columnName].SetOrdinal(columnIndex);
                columnIndex++;
            }
        }
    }

}

