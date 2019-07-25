// Copyright(c) Microsoft Corporation
// All rights reserved.
//
/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace openCypherTranspiler.LogicalPlanner.Utils
{
    public static class ArrayExt
    {
        private static T Max<T>(T x, T y, IComparer<T> comparer)
        {
            if (comparer == null)
            {
                comparer = Comparer<T>.Default;
            }
            return (comparer.Compare(x, y) > 0) ? x : y;
        }

        private static T Min<T>(T x, T y, IComparer<T> comparer)
        {
            if (comparer == null)
            {
                comparer = Comparer<T>.Default;
            }
            return (comparer.Compare(x, y) < 0) ? x : y;
        }

        private static T[,] TransitiveClosureOnce<T>(this T[,] graph, IComparer<T> comparer = null) where T : IComparable
        {
            Debug.Assert(graph.GetLength(0) == graph.GetLength(1));
            var dim = graph.GetLength(0);

            var reach = new T[dim, dim];
            int i, j, k;
            for (i = 0; i < dim; i++)
            {
                for (j = 0; j < dim; j++)
                {
                    reach[i, j] = graph[i, j];
                }
            }
            for (k = 0; k < dim; k++)
            {
                for (i = 0; i < dim; i++)
                {
                    for (j = 0; j < dim; j++)
                    {
                        reach[i, j] = Max(reach[i, j], Min(reach[i, k], reach[k, j], comparer), comparer);
                    }
                }
            }

            return reach;
        }

        public static T[,] TransitiveClosure<T>(this T[,] graph, IComparer<T> comparer = null) where T : IComparable
        {
            return graph.TransitiveClosure(Math.Max(graph.GetLength(0), graph.GetLength(1)), comparer);
        }

        public static T[,] TransitiveClosure<T>(this T[,] graph, int folds, IComparer<T> comparer = null) where T: IComparable
        {
            var reach = graph;
            for (var i = 0; i < folds; i++)
            {
                var newReach = reach.TransitiveClosureOnce(comparer);
                var equal = Enumerable.Range(0, newReach.Rank).All(dimension => newReach.GetLength(dimension) == reach.GetLength(dimension))
                    && newReach.Cast<T>().SequenceEqual(reach.Cast<T>());
                if (equal)
                {
                    return newReach;
                }
                else
                {
                    reach = newReach;
                }
            }
            return reach;
        }
    }
}
