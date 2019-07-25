/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace openCypherTranspiler.Common.Utils
{
    public static class TextHelper
    {
        /// <summary>
        /// Helper function to use a regex pattern to clean up a dirty string for 
        /// certain restriction by another system/api
        /// </summary>
        /// <param name="originalStr"></param>
        /// <param name="regexPat"></param>
        /// <returns></returns>
        public static string MakeCompliantString(string originalStr, string regexPat)
        {
            string compStr = "";
            foreach (Match m in Regex.Matches(originalStr, regexPat))
            {
                compStr += m.Value;
            }
            return compStr;
        }

        /// <summary>
        /// Add or remove indentation at line level
        /// </summary>
        /// <param name="orgStr"></param>
        /// <param name="delta">Positive means adding indentation of delta unit, Negative means remove indentation of delta unit</param>
        /// <param name="indentChar"></param>
        /// <param name="indentUnitCnt"></param>
        /// <returns></returns>
        public static string ChangeIndentation(this string text, int delta, char indentChar = ' ', int indentUnitCnt = 4)
        {
            var lines = Regex.Split(text, "\r\n|\r|\n");

            if (delta > 0)
            {
                var indentation = new string(indentChar, delta * indentUnitCnt);
                return string.Join("\r\n", lines.Select(l => $"{indentation}{l}"));
            }
            else if (delta < 0)
            {
                var indentationUnit = new string(indentChar, indentUnitCnt);
                return string.Join("\r\n", lines.Select(l => Regex.Replace(l, $"^({indentationUnit}){{0,{delta}}}", string.Empty)));
            }
            else
            {
                return text;
            }
        }
    }
}
