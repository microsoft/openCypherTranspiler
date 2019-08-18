/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Linq;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// Represents a reference to a property (column), e.g. r.Score
    /// </summary>
    public class QueryExpressionProperty : QueryExpression
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return Enumerable.Empty<TreeNode>();
            }
        }
        #endregion Implements TreeNode

        /// <summary>
        /// For a property reference, this is the variable part, namely alias of alias.field
        /// </summary>
        public string VariableName { get; set; }

        /// <summary>
        /// For a property reference, this is the property part, namely alias of alias.field
        /// </summary>
        public string PropertyName { get; set; }


        /// <summary>
        /// For a node/relationship, this is the entity type reference
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public Entity Entity { get; set; }

        /// <summary>
        /// For a single field, this is the field's data type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public Type DataType { get; set; }

        public override string ToString()
        {
            if (Entity != null)
            {
                return $"ExprProperty: {VariableName} {Entity}";
            }
            else
            {
                return $"ExprProperty: {VariableName}.{PropertyName}";
            }
        }

        public override Type EvaluateType()
        {
            return DataType;
        }

    }
}
