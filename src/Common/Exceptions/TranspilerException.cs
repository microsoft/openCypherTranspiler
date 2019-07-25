/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;

namespace openCypherTranspiler.Common.Exceptions
{
    /// <summary>
    /// General exception thrown by the Transpiler
    /// </summary>
    public abstract class TranspilerException : Exception
    {
        public TranspilerException(string exceptionMsg) :
            base(exceptionMsg)
        {

        }
    }
}
