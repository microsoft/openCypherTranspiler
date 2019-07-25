/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


namespace openCypherTranspiler.Common.Exceptions
{
    /// <summary>
    /// Exception representing unexpected internal errors thrown by the transpiler
    /// </summary>
    public class TranspilerInternalErrorException : TranspilerException
    {
        public TranspilerInternalErrorException(string intErrMsg) : base($"Unexpected internal error: {intErrMsg}")
        {
        }
    }
}
