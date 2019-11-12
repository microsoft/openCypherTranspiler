/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

namespace openCypherTranspiler.Common.Exceptions
{
    /// <summary>
    /// Exception during data source binding
    /// </summary>
    public class TranspilerBindingException : TranspilerException
    {
        public TranspilerBindingException(string bindingErrorMsg) :
            base($"Data binding error: {bindingErrorMsg}")
        {

        }
    }
}
