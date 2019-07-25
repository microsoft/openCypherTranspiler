/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Linq;
using System.Text;
using openCypherTranspiler.Common.Utils;

namespace openCypherTranspiler.openCypherParser.AST
{
    public abstract class TreeNode
    {
        protected abstract IEnumerable<TreeNode> Children { get; }

        /// <summary>
        /// Debugging purpose: dump the tree in textual format into a string
        /// </summary>
        /// <param name="depth"></param>
        /// <returns></returns>
        public virtual string DumpTree(int depth = 0)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"+{this.GetType().Name}".ChangeIndentation(depth));
            sb.AppendLine($"|{this.ToString()}".ChangeIndentation(depth));
            foreach (var child in Children ?? Enumerable.Empty<TreeNode>())
            {
                sb.Append(child.DumpTree(depth + 1));
            }
            return sb.ToString();
        }

        /// <summary>
        /// Traversal helper to retrieve node of certain type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IEnumerable<T> GetChildrenOfType<T>() where T : TreeNode
        {
            if (this is T)
            {
                return new List<T>(1) { this as T }
                    .Union(Children?.SelectMany(c => c.GetChildrenOfType<T>()) ?? Enumerable.Empty<T>());
            }
            else
            {
                return Children?
                        .SelectMany(c => c.GetChildrenOfType<T>()) ?? Enumerable.Empty<T>();
            }
        }
    }
}
