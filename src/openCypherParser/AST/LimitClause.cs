/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Linq;

namespace openCypherTranspiler.openCypherParser.AST
{

    /// <summary>
    /// Represent a LIMIT clause to limit the returned record count
    /// </summary>
    public class LimitClause : TreeNode
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return new List<TreeNode>() { };
            }
        }
        #endregion Implements TreeNode

        /// <summary>
        /// stores how many rows to keep in "LIMIT" clause
        /// </summary>
        public int RowCount { get; set; }
                
        public override string ToString()
        {
            return $"Limit: n={RowCount}";
        }
    }
}
