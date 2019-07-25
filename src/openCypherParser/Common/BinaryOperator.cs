/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Linq;

namespace openCypherTranspiler.openCypherParser.Common
{
    public enum BinaryOperator
    {
        Invalid = 0,

        // numerical
        Plus,
        Minus,
        Multiply,
        Divide,
        Modulo,
        Exponentiation,

        // logical
        AND,
        OR,
        XOR,

        // comparison
        LT,
        LEQ,
        GT,
        GEQ,
        EQ,
        NEQ,
        REGMATCH,
        IN
    }

    public enum BinaryOperatorType
    {
        Invalid,
        Value,  // takes value type (string or numeric) and output value type
        Logical, // takes logical type (bool) and output logical type
        Comparison // takes value type (string or numeric) and output logical type
    }

    public class BinaryOperatorInfo
    {
        public BinaryOperatorInfo(BinaryOperator name, BinaryOperatorType type)
        {
            Name = name;
            Type = type;
        }
        public BinaryOperator Name { get; private set; }
        public BinaryOperatorType Type { get; private set; }

        public override string ToString()
        {
            return Name.ToString();
        }
    }

    public class OperatorHelper
    {
        private static Dictionary<string, BinaryOperatorInfo> Operators = new Dictionary<string, BinaryOperatorInfo>()
        {
            { "+", new BinaryOperatorInfo(name: BinaryOperator.Plus,type: BinaryOperatorType.Value) },
            { "-", new BinaryOperatorInfo(name: BinaryOperator.Minus, type: BinaryOperatorType.Value) },
            { "*", new BinaryOperatorInfo(name: BinaryOperator.Multiply, type: BinaryOperatorType.Value) },
            { "/", new BinaryOperatorInfo(name: BinaryOperator.Divide, type: BinaryOperatorType.Value) },
            { "%", new BinaryOperatorInfo(name: BinaryOperator.Modulo, type: BinaryOperatorType.Value) },
            { "^", new BinaryOperatorInfo(name: BinaryOperator.Exponentiation, type: BinaryOperatorType.Value) },

            { "<>", new BinaryOperatorInfo(name: BinaryOperator.NEQ, type: BinaryOperatorType.Comparison) },
            { "=", new BinaryOperatorInfo(name: BinaryOperator.EQ, type: BinaryOperatorType.Comparison) },
            { "<", new BinaryOperatorInfo(name: BinaryOperator.LT, type: BinaryOperatorType.Comparison) },
            { ">", new BinaryOperatorInfo(name: BinaryOperator.GT, type: BinaryOperatorType.Comparison) },
            { "<=", new BinaryOperatorInfo(name: BinaryOperator.LEQ, type: BinaryOperatorType.Comparison) },
            { ">=", new BinaryOperatorInfo(name: BinaryOperator.GEQ, type: BinaryOperatorType.Comparison) },
            { "=~", new BinaryOperatorInfo(name: BinaryOperator.REGMATCH, type: BinaryOperatorType.Comparison) },
            { "in", new BinaryOperatorInfo(name: BinaryOperator.IN, type: BinaryOperatorType.Comparison) },

            { "and", new BinaryOperatorInfo(name: BinaryOperator.AND, type: BinaryOperatorType.Logical) },
            { "or", new BinaryOperatorInfo(name: BinaryOperator.OR, type: BinaryOperatorType.Logical) },
            { "xor", new BinaryOperatorInfo(name: BinaryOperator.XOR, type: BinaryOperatorType.Logical) },
        };

        public static BinaryOperatorInfo TryGetOperator(string op)
        {
            string lcOperator = op?.ToLower();
            BinaryOperatorInfo opInfo;

            if (Operators.TryGetValue(lcOperator, out opInfo))
            {
                return opInfo;
            }

            return null;
        }

        public static BinaryOperatorInfo GetOperator(BinaryOperator opEnum)
        {
            return Operators.Values.FirstOrDefault(op => op.Name == opEnum);
        }
    }
}
