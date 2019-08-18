/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using Antlr4.Runtime;
using openCypherTranspiler.Common.Interfaces;
using openCypherTranspiler.Common.Logging;
using openCypherTranspiler.openCypherParser.ANTLR;
using openCypherTranspiler.openCypherParser.AST;

namespace openCypherTranspiler.openCypherParser
{
    /// <summary>
    /// This class implements openCypher parser and
    /// constructs an abstract syntax tree
    /// </summary>
    public class OpenCypherParser : IParser
    {
        private ILoggable _logger;

        public OpenCypherParser(ILoggable logger = null)
        {
            _logger = logger;
        }

        public QueryNode Parse(string queryString)
        {
            var lexer = new CypherLexer(new AntlrInputStream(queryString));
            var tokens = new CommonTokenStream(lexer);
            var parser = new CypherParser(tokens);
            var visitor = new CypherVisitor(_logger);
            var result = visitor.Visit(parser.oC_Cypher()) as QueryNode;
            return result;
        }

    }
}
