/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;

namespace openCypherTranspiler.Common.Exceptions
{
    /// <summary>
    /// Exceptions raised from language syntax error
    /// </summary>
    public class TranspilerSyntaxErrorException : Exception
    {
        public TranspilerSyntaxErrorException(string syntaxErrorMsg) :
            base($"Syntax error: {syntaxErrorMsg}")
        {

        }
    }
}
