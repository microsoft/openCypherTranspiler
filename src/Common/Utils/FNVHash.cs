/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Linq;

namespace openCypherTranspiler.Common.Utils
{
    public static class FNVHash
    {
        public static int GetFNVHash(params object[] objs)
        {
            // FNV hash
            unchecked
            {
                int hash = (int)2166136261;
                if (objs != null)
                {
                    foreach (var o in objs)
                    {
                        hash = hash * 23 + (o?.GetHashCode() ?? 0);
                    }
                    return hash;
                }
            }
            return 0;
        }

        public static int GetFNVHash(IEnumerable<object> objs)
        {
            // FNV hash
            unchecked
            {
                int hash = (int)2166136261;
                if (objs != null)
                {
                    foreach (var o in objs)
                    {
                        hash = hash * 23 + (o?.GetHashCode() ?? 0);
                    }
                    return hash;
                }
            }
            return 0;
        }

        public static uint GetFNVHashUInt(params object[] objs)
        {
            int hash = GetFNVHash(objs);
            return (uint)hash; // this will, for example, convert -1 to 4294967295
        }

        public static uint GetFNVHashUInt(IEnumerable<object> objs)
        {
            int hash = GetFNVHash(objs);
            return (uint)hash; // this will, for example, convert -1 to 4294967295
        }

    }
}
