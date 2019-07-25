/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System.Text;

namespace openCypherTranspiler.SQLRenderer
{
    static class SQLRendererHelpers
    {
        public static StringBuilder AppendLine(this StringBuilder sb, int indentUnits, string line)
        {
            return sb.AppendLine($"{new string(' ', indentUnits * 4)}{line}");
        }
    }
}
