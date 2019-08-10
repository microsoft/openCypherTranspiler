/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System;
using System.Diagnostics;
using openCypherTranspiler.Common.Logging;


namespace openCypherTranspiler.CommonTest
{
    public class TestLogger : BaseLogger, ILoggable
    {
        public TestLogger(LoggingLevel logLevel = LoggingLevel.Verbose)
        {
            SetLoggingLevel(logLevel);
        }

        protected override void LogMessage(string msgFormat, params object[] msgArgs)
        {
            Console.WriteLine(msgFormat, msgArgs);
            Debug.WriteLine(msgFormat, msgArgs);
        }
    }
}
