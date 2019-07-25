/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Linq;
using openCypherTranspiler.Common.Exceptions;

namespace openCypherTranspiler.openCypherParser.Common
{
    public enum Function
    {
        Invalid = 0,

        // Unary operators
        Positive,
        Negative,
        Not,

        // Type conversion functions:
        ToFloat,
        ToString,
        ToBoolean,
        ToInteger,
        ToLong, // non standard oC functions but supported by us
        ToDouble, // non standard oC functions but supported by us
        
        // String functions
        StringStartsWith,
        StringEndsWith,
        StringContains,
        StringLeft,
        StringRight,
        StringTrim,
        StringLTrim,
        StringRTrim,
        StringToUpper,
        StringToLower,
        StringSize,

        // misc functions:
        IsNull,
        IsNotNull
    }

    public class FunctionInfo
    {
        public Function FunctionName { get; set; }
        public int RequiredParameters { get; set; }
        public int OptionalParameters { get; set; }

        /// <summary>
        /// A function that 
        /// </summary>
        public Action<FunctionInfo, IEnumerable<Type>> ParameterChecker { get; set; }
    }

    public class FunctionHelper
    {
        private static void EnsureParameterCount(FunctionInfo info, int actCnt)
        {
            var paramCountMin = info.RequiredParameters;
            var paramCountMax = paramCountMin + info.OptionalParameters;
            if (actCnt <= paramCountMin || actCnt > paramCountMax)
            {
                throw new TranspilerSyntaxErrorException($"Function {info.FunctionName} expects {paramCountMin}{(paramCountMax > paramCountMin ? $" - {paramCountMax}" : "")} parameter(s)");
            }
        }

        private static void EnsureNumericType(FunctionInfo info, Type type)
        {
            if (!(
                type == typeof(int) || type == typeof(int?) ||
                type == typeof(long) || type == typeof(long?) ||
                type == typeof(float) || type == typeof(float?) ||
                type == typeof(double) || type == typeof(double?)
                ))
            {
                throw new TranspilerNotSupportedException($"Function {info.FunctionName} parameter of type {type.Name}");
            }
        }

        private static void EnsureBooleanType(FunctionInfo info, Type type)
        {
            if (!(
                type == typeof(bool) || type == typeof(bool?)
                ))
            {
                throw new TranspilerNotSupportedException($"Function {info.FunctionName} parameter of type {type.Name}");
            }
        }

        private static Action<FunctionInfo, IEnumerable<Type>> EnsureParameterCountChecker = (info, types) =>
        {
            EnsureParameterCount(info, types.Count());
        };

        private static Dictionary<string, FunctionInfo> _funcInfoMap = new Dictionary<string, FunctionInfo>()
        {
            { "+",  new FunctionInfo()
                {
                    FunctionName = Function.Positive,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = (info, types) =>
                    {
                        EnsureParameterCount(info, types.Count());
                        EnsureNumericType(info, types.First());
                    }
                }
            },
            { "-",  new FunctionInfo()
                {
                    FunctionName = Function.Negative,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = (info, types) =>
                    {
                        EnsureParameterCount(info, types.Count());
                        EnsureNumericType(info, types.First());
                    }
                }
            },
            { "not",  new FunctionInfo()
                {
                    FunctionName = Function.Not,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = (info, types) =>
                    {
                        EnsureParameterCount(info, types.Count());
                        EnsureBooleanType(info, types.First());
                    }
                }
            },
            { "tofloat",  new FunctionInfo()
                {
                    FunctionName = Function.ToFloat,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "tostring",  new FunctionInfo()
                {
                    FunctionName = Function.ToString,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "toboolean",  new FunctionInfo()
                {
                    FunctionName = Function.ToBoolean,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "tointeger",  new FunctionInfo()
                {
                    FunctionName = Function.ToInteger,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "tolong",  new FunctionInfo()
                {
                    FunctionName = Function.ToLong,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "todouble",  new FunctionInfo()
                {
                    FunctionName = Function.ToDouble,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "stringstartswith",  new FunctionInfo()
                {
                    FunctionName = Function.StringStartsWith,
                    RequiredParameters = 2,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "stringendswith",  new FunctionInfo()
                {
                    FunctionName = Function.StringEndsWith,
                    RequiredParameters = 2,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "stringcontains",  new FunctionInfo()
                {
                    FunctionName = Function.StringContains,
                    RequiredParameters = 2,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "isnull",  new FunctionInfo()
                {
                    FunctionName = Function.IsNull,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "isnotnull",  new FunctionInfo()
                {
                    FunctionName = Function.IsNotNull,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "left", new FunctionInfo()
                {
                    FunctionName = Function.StringLeft,
                    RequiredParameters = 2,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "right", new FunctionInfo()
                {
                    FunctionName = Function.StringRight,
                    RequiredParameters = 2,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "trim", new FunctionInfo()
                {
                    FunctionName = Function.StringTrim,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "ltrim", new FunctionInfo()
                {
                    FunctionName = Function.StringLTrim,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "rtrim", new FunctionInfo()
                {
                    FunctionName = Function.StringRTrim,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "toupper", new FunctionInfo()
                {
                    FunctionName = Function.StringToUpper,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "tolower", new FunctionInfo()
                {
                    FunctionName = Function.StringToLower,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
            { "size", new FunctionInfo()
                {
                    FunctionName = Function.StringSize,
                    RequiredParameters = 1,
                    OptionalParameters = 0,
                    ParameterChecker = EnsureParameterCountChecker
                }
            },
        };

        /// <summary>
        /// Helper to check if a given function is supported or not
        /// </summary>
        /// <param name="functionName"></param>
        /// <exception cref="TranspilerNotSupportedException">throws not supported exception if a function is not supported</exception>
        /// <returns>Return function info object, or if no match, return null</returns>
        public static FunctionInfo TryGetFunctionInfo(string functionName)
        {
            string lowerCaseFunctionName = functionName.ToLower();
            FunctionInfo funcInfo = null;

            if (_funcInfoMap.TryGetValue(functionName?.ToLower(), out funcInfo))
            {
                return funcInfo;
            }

            return null;
        }

        public static FunctionInfo GetFunctionInfo(Function functionNameEnum)
        {
            return _funcInfoMap.Values.FirstOrDefault(fi => fi.FunctionName == functionNameEnum);
        }

    }
}
