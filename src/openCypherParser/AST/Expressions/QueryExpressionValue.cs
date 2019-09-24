/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Linq;
using openCypherTranspiler.Common.Exceptions;

namespace openCypherTranspiler.openCypherParser.AST
{


    /// <summary>
    /// represents a literal value, such as a string, or number
    /// </summary>
    public class QueryExpressionValue : QueryExpression
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

        private object _value;

        /// <summary>
        /// Holding the object value in a supported value-type
        /// </summary>
        public object Value
        {
            get
            {
                return _value;
            }
            set
            {
                if (!(value is string ||
                      value is bool ||
                      value is int ||
                      value is long ||
                      value is float ||
                      value is double ||
                      value is DateTime ||
                      value is sbyte ||
                      value is byte ||
                      value is short ||
                      value is ushort ||
                      value is uint ||
                      value is ulong ))
                {
                    throw new TranspilerNotSupportedException($"Type {value.GetType()}");
                }
                _value = value;
            }
        }

        public Type ValueType
        {
            get
            {
                return Value.GetType();
            }
        }
        public bool BoolValue
        {
            get
            {
                return (Value is bool ? (bool)Value : Convert.ToBoolean(Value));
            }
            set
            {
                Value = value;
            }
        }
        public int IntValue
        {
            get
            {
                return (Value is int ? (int)Value : Convert.ToInt32(Value));
            }
            set
            {
                Value = value;
            }
        }
        public long LongValue
        {
            get
            {
                return (Value is long ? (long)Value : Convert.ToInt64(Value));
            }
            set
            {
                Value = value;
            }
        }
        public double DoubleValue
        {
            get
            {
                return (Value is double ? (double)Value : Convert.ToDouble(Value));
            }
            set
            {
                Value = value;
            }
        }
        public DateTime DateTimeValue
        {
            get
            {
                return (Value is DateTime ? (DateTime)Value : Convert.ToDateTime(Value));
            }
            set
            {
                Value = value;
            }
        }
        public string StringValue
        {
            get
            {
                return (Value is string ? (string)Value : Value.ToString());
            }
            set
            {
                Value = value;
            }
        }

        public override string ToString()
        {
            return $"ExprValue: t='{Value?.GetType()}'; v='{Value?.ToString()}'";
        }

        public override Type EvaluateType()
        {
            return ValueType;
        }
    }
}
