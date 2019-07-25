/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


namespace openCypherTranspiler.Common.Logging
{
    public abstract class BaseLogger : ILoggable
    {
        protected LoggingLevel _currentLogLevel = LoggingLevel.Normal;

        public void SetLoggingLevel(LoggingLevel logLevel)
        {
            _currentLogLevel = logLevel;
        }

        public void Log(string msgFormat, params object[] msgArgs)
        {
            if (_currentLogLevel >= LoggingLevel.Normal)
            {
                LogMessage(msgFormat, msgArgs);
            }
        }

        public void LogCritical(string msgFormat, params object[] msgArgs)
        {
            if (_currentLogLevel >= LoggingLevel.CriticalOnly)
            {
                LogMessage(msgFormat, msgArgs);
            }
        }

        public void LogVerbose(string msgFormat, params object[] msgArgs)
        {
            if (_currentLogLevel >= LoggingLevel.Verbose)
            {
                LogMessage(msgFormat, msgArgs);
            }
        }

        public void LogCritical(string msg)
        {
            LogCritical("{0}", msg);
        }

        public void Log(string msg)
        {
            Log("{0}", msg);
        }

        public void LogVerbose(string msg)
        {
            LogVerbose("{0}", msg);
        }

        public void LogFuncVerbose(
            string msg,
            [System.Runtime.CompilerServices.CallerMemberName] string memberName = "",
            [System.Runtime.CompilerServices.CallerFilePath] string sourceFilePath = "",
            [System.Runtime.CompilerServices.CallerLineNumber] int sourceLineNumber = 0
            )
        {
            if (_currentLogLevel >= LoggingLevel.Verbose)
            {
                LogMessage("{0}: {1}", memberName, msg);
            }
        }

        public void LogFunc(
            string msg,
            [System.Runtime.CompilerServices.CallerMemberName] string memberName = "",
            [System.Runtime.CompilerServices.CallerFilePath] string sourceFilePath = "",
            [System.Runtime.CompilerServices.CallerLineNumber] int sourceLineNumber = 0
            )
        {
            if (_currentLogLevel >= LoggingLevel.Normal)
            {
                LogMessage("{0}: {1}", memberName, msg);
            }
        }

        public void LogFuncCritical(string msg,
            [System.Runtime.CompilerServices.CallerMemberName] string memberName = "",
            [System.Runtime.CompilerServices.CallerFilePath] string sourceFilePath = "",
            [System.Runtime.CompilerServices.CallerLineNumber] int sourceLineNumber = 0
            )
        {
            LogMessage("{0}: {1}", memberName, msg);
        }

        /// <summary>
        /// This is the only method that child logger class need to implement
        /// </summary>
        /// <param name="msgFormat"></param>
        /// <param name="msgArgs"></param>
        abstract protected void LogMessage(string msgFormat, params object[] msgArgs);
    }
}
