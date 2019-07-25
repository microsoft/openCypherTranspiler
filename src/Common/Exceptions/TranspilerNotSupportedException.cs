/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;

namespace openCypherTranspiler.Common.Exceptions
{
    /// <summary>
    /// Specific exception thrown by scenarios that were explicitly blocked
    /// </summary>
    public class TranspilerNotSupportedException : Exception
    {
        public TranspilerNotSupportedException(string functionNotSupported) :
            base($"{functionNotSupported} is not supported by the code generator.")
        {

        }
    }
}
