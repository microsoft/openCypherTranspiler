/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace openCypherTranspiler.Common.Utils
{
    public class TypeHelper
    {
        /// <summary>
        /// Check if null value can be assigned
        /// E.g. IsNullableType(string) = true, IsNullableType(DateTime) = false
        ///      IsNullableType(
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static bool CanAssignNullToType(Type type)
        {
            if (!type.IsValueType)
            {
                return true; // ref-type can assign null value
            }
            if (IsSystemNullableType(type))
            {
                return true; // Nullable<T> can assign null value
            }
            return false;
        }

        /// <summary>
        /// Check if a type is System.Nullable<T> or derived from System.Nullable<T>
        /// E.g. IsNullableType(string) = false, IsNullableType(System.Nullable<int>) = true
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static bool IsSystemNullableType(Type type)
        {
            // NOTE: Nullable<T> is sealed type, so no class can derive from it
            if (type != default(Type) && Nullable.GetUnderlyingType(type) != null)
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Given an non-nullable type, wrap it in a System.Nullable<> generic type
        /// </summary>
        /// <param name="type">Non nullable type</param>
        /// <returns></returns>
        /// <exception cref="InvalidCastException">Throws InvalidCastException, if type is already nullable.</exception>  
        public static Type GetNullableTypeForType(Type type)
        {
            if (CanAssignNullToType(type))
            {
                throw new InvalidCastException($"Type: {type} is already nullable");
            }
            return typeof(Nullable<>).MakeGenericType(type);
        }

        /// <summary>
        /// Check if a type is derived from a generic type
        /// E.g. class SubClass : Node<int> {} ;
        ///      IsSubclassOfRawGeneric(typeof(SubClass), Node<>) == true
        /// </summary>
        /// <param name="generic"></param>
        /// <param name="toCheck"></param>
        /// <returns></returns>
        public static bool IsSubclassOfRawGeneric(Type toCheck, Type generic)
        {
            while (toCheck != null && toCheck != typeof(object))
            {
                var cur = toCheck.IsGenericType ? toCheck.GetGenericTypeDefinition() : toCheck;
                if (generic == cur)
                {
                    return true;
                }
                toCheck = toCheck.BaseType;
            }
            return false;
        }

        /// <summary>
        /// Return the type itself, or for a nullable type, its unboxed type
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static Type GetUnderlyingTypeIfNullable(Type t)
        {
            var isNullable = IsSystemNullableType(t);
            return isNullable ? Nullable.GetUnderlyingType(t) : t;
        }

        /// <summary>
        /// Wrap unnullable type to nullable
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static Type MakeNullableIfNotAlready(Type t)
        {
            if (!CanAssignNullToType(t))
            {
                return typeof(Nullable<>).MakeGenericType(t);
            }
            else
            {
                return t;
            }
        }
    }
}
