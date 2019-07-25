/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using Antlr4.Runtime;
using openCypherTranspiler.Common.Logging;
using openCypherTranspiler.openCypherParser.ANTLR;
using openCypherTranspiler.openCypherParser.AST;

namespace openCypherTranspiler.openCypherParser
{

    public class OpenCypherParser
    {
        private OpenCypherParser()
        { }

        public static QueryTreeNode Parse(string cypherQueryText, ILoggable logger = null)
        {
            var lexer = new CypherLexer(new AntlrInputStream(cypherQueryText));
            var tokens = new CommonTokenStream(lexer);
            var parser = new CypherParser(tokens);
            var visitor = new CypherVisitor(logger);
            var result = visitor.Visit(parser.oC_Cypher()) as QueryTreeNode;
            return result;
        }

    }
}
