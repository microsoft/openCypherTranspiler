/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


namespace openCypherTranspiler.Common.Logging
{
    public enum LoggingLevel
    {
        CriticalOnly,
        Normal,
        Verbose,
    }

    /// <summary>
    /// Inteface for the logger used through out this project
    /// </summary>
    public interface ILoggable
    {
        void SetLoggingLevel(LoggingLevel logLevel);


        // LogFunc* calls with log the msg with the function name as well

        void LogFuncCritical(
            string msg,
            [System.Runtime.CompilerServices.CallerMemberName] string memberName = "",
            [System.Runtime.CompilerServices.CallerFilePath] string sourceFilePath = "",
            [System.Runtime.CompilerServices.CallerLineNumber] int sourceLineNumber = 0
            );
        void LogFunc(
            string msg,
            [System.Runtime.CompilerServices.CallerMemberName] string memberName = "",
            [System.Runtime.CompilerServices.CallerFilePath] string sourceFilePath = "",
            [System.Runtime.CompilerServices.CallerLineNumber] int sourceLineNumber = 0
            );
        void LogFuncVerbose(
            string msg,
            [System.Runtime.CompilerServices.CallerMemberName] string memberName = "",
            [System.Runtime.CompilerServices.CallerFilePath] string sourceFilePath = "",
            [System.Runtime.CompilerServices.CallerLineNumber] int sourceLineNumber = 0
            );
        
        void LogCritical(string msg);
        void LogCritical(string msgFormat, params object[] msgArgs);
        void Log(string msg);
        void Log(string msgFormat, params object[] msgArgs);
        void LogVerbose(string msg);
        void LogVerbose(string msgFormat, params object[] msgArgs);
    }
}
