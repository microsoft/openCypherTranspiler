/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Antlr4.Runtime;
using Antlr4.Runtime.Misc;
using Antlr4.Runtime.Tree;
using System.Text.RegularExpressions;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.Common.Logging;
using openCypherTranspiler.openCypherParser.ANTLR;
using openCypherTranspiler.openCypherParser.AST;
using openCypherTranspiler.openCypherParser.Common;
using openCypherTranspiler.Common.Utils;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// openCypher's ANTLR4 visitor to turn parsing result CST to AST
    /// </summary>
    internal class CypherVisitor : CypherBaseVisitor<object>
    {
        private ILoggable _logger;

        private class Optional<T> where T : ParserRuleContext { }

        #region Helper functions

        // check existence of parent nodes
        private bool IsInsideContextType(Type type, RuleContext ctx)
        {
            var parentObj = ctx?.Parent;
            while (parentObj != null)
            {
                if (parentObj.GetType() == type)
                {
                    return true;
                }
                parentObj = parentObj?.Parent;
            }
            return false;
        }

        private bool IsContextContainsTextToken(IParseTree context, string textToken)
        {
            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);
                if (child is ITerminalNode && string.Compare(child.GetText(), textToken, true) == 0)
                {
                    return true;
                }
            }
            return false;
        }

        private bool IsContextSP(IParseTree context)
        {
            return (context is ITerminalNode && (context as ITerminalNode).Symbol.Type == CypherParser.SP);
        }

        // 'take' the current state variable as a value but at same time reset it
        private T TakeAndResetStateVariable<T>(ref T variable)
        {
            T tempValue = variable;
            variable = default(T);
            return tempValue;
        }

        // helper function to see if a context node's children match specfic pattern (useful when it supports more than one type of patterns)
        private bool IsMatchSequence(ParserRuleContext ctx, params object[] seq)
        {
            List<object> ctxChildren = new List<object>();
            for (int i = 0; i < ctx.ChildCount; i++)
            {
                var ctxChild = ctx.GetChild(i);
                if (ctxChild is ITerminalNode)
                {
                    if (!IsContextSP(ctxChild))
                    {
                        ctxChildren.Add(ctxChild.GetText());
                    }
                }
                else
                {
                    ctxChildren.Add(ctxChild.GetType());
                }
            }
            string matchSrc = string.Join("|", ctxChildren.Select(o => o is string ? (string)o : ((Type)o).Name)) + "|";
            string matchPat = string.Join("",
                seq.Select(o => o is string ? $"{Regex.Escape((string)o)}\\|" :
                    TypeHelper.IsSubclassOfRawGeneric((Type)o, typeof(Optional<>)) ? $"(?:{Regex.Escape(((Type)o).Name)}\\|)?" : $"{Regex.Escape(((Type)o).Name)}\\|")
                    );
            return Regex.IsMatch(matchSrc, matchPat);
        }

        private QueryExpressionBinary HandleChainedBinaryExpressions(IParseTree context)
        {
            var parentExpression = new QueryExpressionBinary();
            var supportedOperators = new string[] { "+", "-", "%", "*", "/" };

            bool visitedLeft = false;
            bool visitedOperator = false;
            bool visitedRight = false;


            // get left expression
            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (!visitedLeft && !(child is ITerminalNode))
                {
                    Debug.Assert(
                        // list of supported expressions in this form today:
                        child is CypherParser.OC_AddOrSubtractExpressionContext ||
                        child is CypherParser.OC_MultiplyDivideModuloExpressionContext ||
                        child is CypherParser.OC_PowerOfExpressionContext
                        );

                    var childExpr = Visit(child) as QueryExpression ?? throw new TranspilerSyntaxErrorException($"Expect valid left expression {child.GetText()}");
                    parentExpression.LeftExpression = childExpr;
                    visitedLeft = true;
                    continue;
                }

                if (child is ITerminalNode && supportedOperators.Contains(context.GetChild(i).GetText()))
                {
                    var op = child.GetText();

                    if (parentExpression.Operator != null)
                    {
                        Debug.Assert(parentExpression.RightExpression != null);
                        // not the first right operator/expr pair. do chaining
                        var newParentExpression = new QueryExpressionBinary()
                        {
                            LeftExpression = parentExpression
                        };
                        parentExpression = newParentExpression;
                    }

                    var opEnum = OperatorHelper.TryGetOperator(op)
                        ?? throw new TranspilerSyntaxErrorException($"Unknown or unsupported operator {op}");
                    parentExpression.Operator = opEnum;
                    visitedOperator = true;
                    // reset visited right as soon as we see another new operator
                    visitedRight = false;
                    continue;
                }

                if (!visitedRight && !(child is ITerminalNode))
                {
                    Debug.Assert(visitedOperator);
                    var childExpr = Visit(child) as QueryExpression ?? throw new TranspilerSyntaxErrorException($"Expect valid right expression {child.GetText()}");
                    parentExpression.RightExpression = childExpr;
                    visitedRight = true;
                    continue;
                }
            }

            Debug.Assert(visitedLeft && visitedOperator && visitedRight);
            return parentExpression;
        }

        private QueryExpressionBinary HandlesBinaryExpression(IParseTree context, string op)
        {
            var opEnum = OperatorHelper.TryGetOperator(op)
                        ?? throw new TranspilerSyntaxErrorException($"Unknown or unsupported operator {op}");

            QueryExpression finalExp = null;
            bool visitedLeft = false;

            // get left expression
            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (!visitedLeft && !(child is ITerminalNode))
                {
                    var childExpr = Visit(child) as QueryExpression
                        ?? throw new TranspilerSyntaxErrorException($"Expect valid left side expression {child.GetText()}");
                    finalExp = childExpr;
                    visitedLeft = true;
                    continue;
                }

                if (!(child is ITerminalNode))
                {
                    var childExpr = Visit(child) as QueryExpression
                        ?? throw new TranspilerSyntaxErrorException($"Expect valid right side expression {child.GetText()}");
                    finalExp = new QueryExpressionBinary()
                    {
                        Operator = opEnum,
                        LeftExpression = finalExp,
                        RightExpression = childExpr
                    };
                    continue;
                }
            }

            return finalExp as QueryExpressionBinary
                ?? throw new TranspilerSyntaxErrorException($"Expecting at least 2 sides of a binary operator: {context.GetText()}");
        }

        private QueryExpressionFunction HandlesUnaryExpression(IParseTree context)
        {
            // We treat function and unary expression the same:
            //  e.g. +expr or -expr, or NOT expr
            QueryExpression finalExpr = null;
            var supportedOperators = new string[] { "+", "-'" };
            var unaryOpChain = new Stack<FunctionInfo>();

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (child is ITerminalNode && supportedOperators.Any(op => op == child.GetText()))
                {
                    Debug.Assert(finalExpr == null); // operator should only appear before the expression
                    var fi = FunctionHelper.TryGetFunctionInfo(child.GetText().Trim())
                        ?? throw new TranspilerInternalErrorException("Parsing function info"); // unexpected operator. should have stopped by caller
                    unaryOpChain.Push(fi);
                    continue;
                }

                if (!(child is ITerminalNode))
                {
                    Debug.Assert(finalExpr == null); // expr should only appear once in unary operator
                    finalExpr = Visit(child) as QueryExpression
                        ?? throw new TranspilerSyntaxErrorException($"Expect valid right expression {child.GetText()}");
                }
            }

            Debug.Assert(unaryOpChain.Count > 0);
            Debug.Assert(finalExpr != null);

            while (unaryOpChain.Count > 0)
            {
                finalExpr = new QueryExpressionFunction()
                {
                    Function = unaryOpChain.Pop(),
                    InnerExpression = finalExpr
                };
            }

            return finalExpr as QueryExpressionFunction
                ?? throw new TranspilerSyntaxErrorException($"Expect valid expression for an unary operator at least once: {context.GetText()}");
        }

        private QueryExpressionFunction HandlesUnaryFuncExpression(IParseTree context, string funcName)
        {
            // We treat function and unary expression the same:
            //  e.g. +expr or -expr, or NOT expr
            var fi = FunctionHelper.TryGetFunctionInfo(funcName)
                // unexpected function name: should have been stopped by the caller to HandlesUnaryFuncExpression
                ?? throw new TranspilerInternalErrorException("Parsing function info");

            var parentExpression = new QueryExpressionFunction()
            {
                Function = fi
            };

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (!(child is ITerminalNode))
                {
                    var childExpr = Visit(child) as QueryExpression ?? throw new TranspilerSyntaxErrorException($"Expect valid right expression {child.GetText()}");
                    Debug.Assert(parentExpression.InnerExpression == null);
                    parentExpression.InnerExpression = childExpr;
                }
            }

            return parentExpression;
        }

        /// <summary>
        /// Helper function to get all direct entity reference from a return body item list ( [some expr] AS [alias] )
        /// E.g.  MATCH (p:person) WITH p as q, p.id as Id, this will return just Entity with alias = p and type = person
        /// </summary>
        /// <param name="exprs"></param>
        /// <returns>A dictionary of the reference to the Entity object keyed by its alias (in AS [alias] expr)</returns>
        private IDictionary<string, Entity> GetDirectlyExposedEntities(IEnumerable<QueryExpressionWithAlias> exprs)
        {
            return exprs
                .Select(expr => new { Alias = expr.Alias, Entity = expr.InnerExpression.TryGetDirectlyExposedEntity() })
                .Where(e => e.Entity != null)
                .ToDictionary(expr => expr.Alias, expr => expr.Entity);
        }

        /// <summary>
        /// Helper function to call GetEntitiesFromReturnBodyByRef but then replace the alias with the QueryExpressionWithAlias's alias
        /// E.g.  MATCH (p:person) WITH p as q. This will return entity type person with alias = q (instead of p)
        /// </summary>
        /// <param name="exprs"></param>
        /// <returns></returns>
        private IEnumerable<Entity> GetDirectlyExposedEntitiesWithAliasApplied(IEnumerable<QueryExpressionWithAlias> exprs)
        {
            return GetDirectlyExposedEntities(exprs)
                .Select(kv =>
                {
                    var ent = kv.Value.Clone();
                    ent.Alias = kv.Key;
                    return ent;
                });
        }

        /// <summary>
        /// Helper function to get all non entity fields from a return body item list ( [some expr] AS [alias] )
        /// E.g.  ... WITH avg+1 as shift_avg. This will return QueryExprWithAlias: {alias = shift_avg, expr = avg+1}
        /// </summary>
        /// <param name="exprs"></param>
        /// <returns></returns>
        private IEnumerable<string> GetNonEntitiesAliases(IEnumerable<QueryExpressionWithAlias> exprs)
        {
            return exprs
                .Where(expr => expr.InnerExpression.TryGetDirectlyExposedEntity() == null)
                .Select(expr => expr.Alias);
        }


        private PartialQueryNode CreateQueryPartFromReadingClauses(
            CypherParser.OC_ReadingClauseContext readingClause,
            PartialQueryNode prevQueryPart
            )
        {
            // this function consolidates consolidate multiple matches, each is a list of MatchPattern, and
            // enforce that optional match cannot be the first one
            var matchParts = new List<MatchPattern>(); // consolidate multiple matches, each is a list of MatchPattern

            // returns valuetuple type (pass through):
            //   { MatchPatterns(IList<MatchPattern>), Condition(QueryExpression) }
            var result = ((IList<MatchPattern> MatchPatterns, QueryExpression Condition))Visit(readingClause);
            IList<MatchPattern> matchPatterns = result.MatchPatterns ?? throw new TranspilerInternalErrorException("Parsing oC_ReadingClause");

            var isOptionalMatch = matchPatterns.Any(p => p.IsOptionalMatch);
            Debug.Assert(matchPatterns.All(p => p.IsOptionalMatch == isOptionalMatch));

            if (isOptionalMatch)
            {
                // if optional match, then it cannot be the first reading clause
                if (prevQueryPart == null)
                {
                    throw new TranspilerSyntaxErrorException("First MATCH cannot be OPTIONAL");
                }
            }
            else
            {
                // not optional match, then we cannot follow immediately after optional match
                if (prevQueryPart != null && !prevQueryPart.CanChainNonOptionalMatch)
                {
                    throw new TranspilerSyntaxErrorException("MATCH cannot follow OPTIONAL MATCH (perhaps use a WITH clause between them)");
                }
            }

            matchParts.AddRange(matchPatterns);

            // construct the match data source (representing a group of matches)
            MatchClause matchDs = null;
            if (matchParts.Count > 0)
            {
                matchDs = new MatchClause()
                {
                    MatchPatterns = matchParts
                };
            }

            // A Match statement (not RET or WITH) has implied projection expressions as returned items of 
            // this partial query node:
            //  - includes all named entities in the match pattern (later, we can optimize out if some are 
            //    not used at all)
            //  - includes all inherited returns
            var allReturnedEntities = matchParts.SelectMany(l => l)
                .Union(prevQueryPart != null ? 
                    GetDirectlyExposedEntitiesWithAliasApplied(prevQueryPart.ProjectedExpressions) : 
                    Enumerable.Empty<Entity>())
                .Where(e => !string.IsNullOrEmpty(e.Alias))
                .GroupBy(e => e.Alias);
            var allInheritedProperties = prevQueryPart != null ? 
                GetNonEntitiesAliases(prevQueryPart.ProjectedExpressions) : 
                Enumerable.Empty<string>();

            var returnedExprs = allReturnedEntities
                .Select(g =>
                {
                    if (g.Select(e => e.EntityName).Where(en => !string.IsNullOrEmpty(en)).Distinct().Count() > 1)
                    {
                        throw new TranspilerSyntaxErrorException($"Multiple labels assigned to same alias {g.Key}: {string.Join(",", g.Select(e => e.EntityName).Where(en => !string.IsNullOrEmpty(en)))}");
                    }
                    return new QueryExpressionWithAlias()
                    {
                        Alias = g.Key,
                        InnerExpression = new QueryExpressionProperty()
                        {
                            Entity = g.FirstOrDefault(en => !string.IsNullOrEmpty(en.EntityName)).Clone(),
                            VariableName = g.Key
                        }
                    };
                })
                .Union(
                    allInheritedProperties.Select(alias =>
                        new QueryExpressionWithAlias()
                        {
                            Alias = alias,
                            InnerExpression = new QueryExpressionProperty()
                            {
                                VariableName = alias
                            }
                        }
                    )
                )
                .ToList();

            return new PartialQueryNode()
            {
                MatchData = matchDs,
                FilterExpression = result.Condition,
                ProjectedExpressions = returnedExprs,
                PipedData = prevQueryPart,
                IsDistinct = false,  // MATCH reading clause will not be come with DISTINCT keyword
                CanChainNonOptionalMatch = !isOptionalMatch,
                IsImplicitProjection = true, // MATCH's projection is always implied (returning all aliased entities)
            };
        }

        private SingleQueryNode ConstructSingleQuery([NotNull] CypherParser.OC_SinglePartQueryContext context, PartialQueryNode prevQueryPart)
        {
            Debug.Assert(context.oC_Return() != null);

            SingleQueryNode queryNode = null;

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (child is CypherParser.OC_ReadingClauseContext) // MATCH (WHERE)
                {
                    var partQueryTree = CreateQueryPartFromReadingClauses(
                        child as CypherParser.OC_ReadingClauseContext,
                        prevQueryPart
                        );

                    // set state for next clause
                    prevQueryPart = partQueryTree;
                }
                else if (child is CypherParser.OC_ReturnContext) // RETURN
                {
                    if (prevQueryPart == null)
                    {
                        throw new TranspilerNotSupportedException("RETURN projection without reading clauses (a.k.a. Match)");
                    }

                    var returnResult = ((
                            bool IsDistinct,
                            IList<QueryExpressionWithAlias> ReturnItems,
                            IList<SortItem> OrderByItems,
                            LimitClause Limit))Visit(child);
                    var projectionExprs = returnResult.ReturnItems ?? throw new TranspilerSyntaxErrorException($"Expecting a valid list of items to be returned: {child.GetText()}");
                    bool isDistinct = returnResult.IsDistinct;
                    var singleQueryNode = new SingleQueryNode()
                    {
                        PipedData = prevQueryPart,
                        EntityPropertySelected = projectionExprs,
                        IsDistinct = isDistinct,
                        OrderByClause = returnResult.OrderByItems,
                        Limit = returnResult.Limit,
                    };

                    // Check that returned have no conflicting aliases
                    var firstDup = projectionExprs.GroupBy(expr => expr.Alias).FirstOrDefault(g => g.Count() > 1);
                    if (firstDup != null)
                    {
                        throw new TranspilerSyntaxErrorException($"Multiple result columns with the same name are not supported: {firstDup.Key}");
                    }

                    // propagate the entity types to the list of return items that are direct exposure of entities
                    // NOTE: currently we assume direct reference. Any function manipulation will cause it become
                    //       a non-node reference so that it cannot be used as entity variables in next MATCH statement
                    //       e.g. u as u or (u) as u is okay , AVG(u) as u is no longer an entity
                    UpdateEntityTypesInExpressionInPlace(
                        projectionExprs,
                        GetDirectlyExposedEntitiesWithAliasApplied(prevQueryPart.ProjectedExpressions)
                        );

                    queryNode = singleQueryNode;
                }
                else if (child is CypherParser.OC_UpdatingClauseContext)
                {
                    throw new TranspilerNotSupportedException("Any type of updating clause");
                }
                else
                {
                    // ignore the rest type syntax nodes (e.g. SP)
                }
            }

            return queryNode;
        }

        /// <summary>
        /// Helper update in-place the Entity Type for any QueryExpressionProperty potentially referring to an entity
        /// nested in a list of QueryExpression roots
        /// </summary>
        /// <param name="exprsToUpdate">list of QueryExpression that may contain QueryExpressionProperty inside</param>
        /// <param name="entities">a mapping of entity type to alias</param>
        /// <returns></returns>
        private void UpdateEntityTypesInExpressionInPlace(IEnumerable<QueryExpression> exprsToUpdate, IEnumerable<Entity> entities)
        {
            var projectionExprsReferingEntities = exprsToUpdate
                .SelectMany(e => e.GetChildrenQueryExpressionType<QueryExpressionProperty>())
                .Cast<QueryExpressionProperty>()
                .Where(p => string.IsNullOrEmpty(p.PropertyName))
                .ToList();
            // make best-effort in-place update for any alias with an entity match
            // for any that did not match, it is either a reference to a value alias
            // or a dangling alias - which will trigger exception down the line
            projectionExprsReferingEntities.ForEach(e =>
            {
                var entityMatchedAlias = entities.FirstOrDefault(e2 => e2.Alias == e.VariableName);
                if (entityMatchedAlias != null)
                {
                    e.Entity = entityMatchedAlias.Clone();
                }
            });
        }

        // the order of parsing types referred from following:
        // https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/builtin-types/integral-numeric-types
        QueryExpressionValue ParseIntegerLiteral(string literal)
        {
            // sbyte was excluded due to no equivalent type in sql type mapping
            if (byte.TryParse(literal, out byte byteValue))
            {
                return new QueryExpressionValue()
                {
                    Value = byteValue
                };
            }
            else if (short.TryParse(literal, out short shortValue))
            {
                return new QueryExpressionValue()
                {
                    Value = shortValue
                };
            }
            else if (ushort.TryParse(literal, out ushort ushortValue))
            {
                return new QueryExpressionValue()
                {
                    Value = ushortValue
                };
            }
            else if (int.TryParse(literal, out int intValue))
            {
                return new QueryExpressionValue()
                {
                    Value = intValue
                };
            }
            else if (uint.TryParse(literal, out uint uintValue))
            {
                return new QueryExpressionValue()
                {
                    Value = uintValue
                };
            }
            else if (long.TryParse(literal, out long longValue))
            {
                return new QueryExpressionValue()
                {
                    Value = longValue
                };
            }
            else if (ulong.TryParse(literal, out ulong ulongValue))
            {
                return new QueryExpressionValue()
                {
                    Value = ulongValue
                };
            }
            else
            {
                throw new TranspilerNotSupportedException($"Integer literal :{literal} out of supported range");
            }
        }

        #endregion Helper functions

        public CypherVisitor(ILoggable logger)
        {
            _logger = logger;
        }

        // this method returns the final tree
        public override object VisitOC_Cypher([NotNull] CypherParser.OC_CypherContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Cypher", context.GetText());
            // return the final parsed tree of type QueryTreeNode (pass through)
            Debug.Assert(context.oC_Statement() != null);
            return Visit(context.oC_Statement()) as QueryNode ?? throw new TranspilerInternalErrorException("Parsing oC_Statement");
        }

        public override object VisitOC_Query([NotNull] CypherParser.OC_QueryContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Query", context.GetText());
            if (context.oC_RegularQuery() != null)
            {
                // return the final parsed tree of type QueryTreeNode (pass through)
                return Visit(context.oC_RegularQuery()) as QueryNode ?? throw new TranspilerInternalErrorException("Parsing oc_RegularQuery");
            }
            else
            {
                throw new TranspilerNotSupportedException("CALL statement");
            }
        }

        public override object VisitOC_RegularQuery([NotNull] CypherParser.OC_RegularQueryContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_RegularQuery", context.GetText());
            // returns QueryTreeNode, can be a passed through SingleQueryTreeNode, or InfixQueryTreeNode

            Debug.Assert(context.oC_SingleQuery() != null);
            var rootNode = Visit(context.oC_SingleQuery()) as QueryNode ?? throw new TranspilerInternalErrorException("Parsing oC_SingleQuery");

            if ((context.oC_Union()?.Length ?? 0) > 0)
            {
                foreach (var childCtx in context.oC_Union())
                {
                    Debug.Assert(childCtx.UNION() != null);
                    var unioned = new InfixQueryNode()
                    {
                        LeftQueryTreeNode = rootNode,
                        RightQueryTreeNode = Visit(childCtx.oC_SingleQuery()) as QueryNode ?? throw new TranspilerInternalErrorException("Parsing oC_SingleQuery of UNION"),
                        Operator = childCtx.ALL() != null ? InfixQueryNode.QueryOperator.UnionAll : InfixQueryNode.QueryOperator.Union
                    };
                    rootNode = unioned;
                }
            }
            
            return Visit(context.oC_SingleQuery()) as QueryNode ?? throw new TranspilerInternalErrorException("Parsing oC_SingleQuery");
        }

        public override object VisitOC_SingleQuery([NotNull] CypherParser.OC_SingleQueryContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_SingleQuery", context.GetText());
            // returns SingleQueryTreeNode

            // In finalizing the SingleQueryNode, we ensure on a best-effort basis that all entity references 
            // have its type spelled out, e.g. In
            //   MATCH (a:Person), (a) WITH a ...
            // the subsequence reference to 'a' will be marked as entity type 'Person'
            //
            // To do this, we walk up the tree (bear in mind: bottom of the tree is earlier in the openCypher 
            // query, and root node of the tree is the final result) and in each layer
            //
            // In this implementation, for any types we cannot infer, we throw exception eventually
            var singleQuery = VisitChildren(context) as SingleQueryNode ?? throw new TranspilerInternalErrorException("Parsing oC_SinglePartQuery or oC_MultiPartQuery");

            var queryChainTreeNode = singleQuery.PipedData as PartialQueryNode;
            Debug.Assert(queryChainTreeNode != null); // should be guaranteed by child visitor already
            var queryChain = new Stack<PartialQueryNode>();
            queryChain.Push(queryChainTreeNode);

            while (queryChainTreeNode.PipedData != null)
            {
                queryChainTreeNode = queryChainTreeNode.PipedData;
                queryChain.Push(queryChainTreeNode);
            }

            // propagate entity types for all entity objects sharing same alias for each query part, from
            // bottom up of the query tree
            // for any conflict type (e.g. single alias assigned to 2 types), raise error
            var inheritedEntities = Enumerable.Empty<Entity>();
            while (queryChain.Count > 0)
            {
                var queryTreeNode = queryChain.Pop();
                var entitiesInReturn = GetDirectlyExposedEntities(queryTreeNode.ProjectedExpressions);
                var entitiesInMatchPatterns = queryTreeNode.MatchData?.AllEntitiesOrdered ?? Enumerable.Empty<Entity>().ToList();

                // first pass, propagate all node and edge labels for those sharing aliases
                entitiesInReturn
                    .Values
                    .Union(entitiesInMatchPatterns)
                    .Where(e => !string.IsNullOrEmpty(e.Alias)) // skip over all anonymous node/edge. we later assert that they all have types already specified explicitly 
                    .GroupBy(e => e.Alias)
                    .ToList()
                    .ForEach(g =>
                    {
                        // Ensure that aliases are assigned type and label consistently
                        var entityNames = g.Select(e => e.EntityName).Where(en => !string.IsNullOrEmpty(en)).Distinct();
                        var entityTypes = g.Select(e => e.GetType()).Distinct();

                        if (entityTypes.Count() != 1)
                        {
                            throw new TranspilerSyntaxErrorException($"The same alias '{g.Key}' is assigned to both NODE and RELATIONSHIPS");
                        }
                        if (entityNames.Count() > 1)
                        {
                            throw new TranspilerSyntaxErrorException($"The alias '{g.Key}' assigned to more than one labels {string.Join(", ", entityNames)}");
                        }
                        if (entityNames.Count() == 0)
                        {
                            throw new TranspilerSyntaxErrorException($"Type label for alias '{g.Key}' cannot be deduced. Did you not give a label to it?");
                        }

                        g.ToList().ForEach(e =>
                        {
                            e.EntityName = entityNames.First();
                        });

                    });

                // second pass, loop through all relationships and update the the source and sink entity name
                for (int i = 0; i < entitiesInMatchPatterns.Count; i++)
                {
                    var relEnt = entitiesInMatchPatterns[i] as RelationshipEntity;

                    if (relEnt != null)
                    {
                        var prevEnt = entitiesInMatchPatterns[i - 1] as NodeEntity;
                        var nextEnt = entitiesInMatchPatterns[i + 1] as NodeEntity;
                        Debug.Assert(prevEnt != null && nextEnt != null);
                        relEnt.LeftEntityName = prevEnt.EntityName;
                        relEnt.RightEntityName = nextEnt.EntityName;
                    }
                }

                // third pass, update all relationship entities' in/out node names
                entitiesInReturn
                    .Values
                    .Union(inheritedEntities)
                    .Where(e => e is RelationshipEntity)
                    .Union(entitiesInMatchPatterns.Where(e => e is RelationshipEntity))
                    .Where(e => !string.IsNullOrEmpty(e.Alias)) // skip over anonymous edges . we later assert that they all have types already specified explictly 
                    .GroupBy(e => e.Alias)
                    .ToList()
                    .ForEach(g =>
                    {
                        var entityTypes = g.Select(e => e.GetType()).Distinct();
                        Debug.Assert(entityTypes.Count() == 1);

                        if (entityTypes.First() == typeof(RelationshipEntity))
                        {
                            var entityFromNames = g.Select(e => (e as RelationshipEntity).LeftEntityName).Where(en => !string.IsNullOrEmpty(en)).Distinct();
                            var entityToNames = g.Select(e => (e as RelationshipEntity).RightEntityName).Where(en => !string.IsNullOrEmpty(en)).Distinct();

                            if (entityFromNames.Count() > 1 || entityToNames.Count() > 1)
                            {
                                var entNames = g.Select(e => $"({(e as RelationshipEntity).LeftEntityName})-[{e.EntityName}]-({(e as RelationshipEntity).RightEntityName})").Distinct();
                                throw new TranspilerSyntaxErrorException($"The alias {g.Key} assigned to a different type of relationships: {string.Join(", ", entNames)}");
                            }

                            g.Cast<RelationshipEntity>().ToList().ForEach(e =>
                            {
                                e.LeftEntityName = entityFromNames.First();
                                e.RightEntityName = entityToNames.First();
                            });
                        }
                    });

                // final pass: verify that all types are inferred
                var firstUnknownTypeEntity = entitiesInReturn.Values
                    .FirstOrDefault(e => e is NodeEntity ?
                        string.IsNullOrEmpty((e as NodeEntity).EntityName) :
                        string.IsNullOrEmpty((e as RelationshipEntity).EntityName) || string.IsNullOrEmpty((e as RelationshipEntity).LeftEntityName) || string.IsNullOrEmpty((e as RelationshipEntity).RightEntityName)
                        );
                if (firstUnknownTypeEntity != null)
                {
                    throw new TranspilerSyntaxErrorException($"Type label for alias '{firstUnknownTypeEntity.Alias}' cannot be deduced. Did you not give a label to it?");
                }

                // before moving to next query part, update inheritedEntities to the current query part's returned list of entities
                inheritedEntities = GetDirectlyExposedEntitiesWithAliasApplied(queryTreeNode.ProjectedExpressions);
            }

            // in the final return statement, we currently do not support return entities, so we fail any attempt to do so
            var entitiesInFinalReturn = GetDirectlyExposedEntitiesWithAliasApplied(singleQuery.EntityPropertySelected);
            if (entitiesInFinalReturn.Count() > 0)
            {
                throw new TranspilerNotSupportedException($"Entities ({string.Join(", ", entitiesInFinalReturn.Select(e => e.Alias))}) in return statement");
            }

            return singleQuery;
        }

        public override object VisitOC_MultiPartQuery([NotNull] CypherParser.OC_MultiPartQueryContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_MultiPartQuery", context.GetText());
            
            // Multipart query has several situations we need to handle
            //   - MATCH not followed by any with but termination  (MATCH RETURN)
            //   - with immediately follows a MATCH (MATCH WITH RETURN)
            //   - with follows other WITH (MATCH WITH WITH RETURN)
            //   - standalone WITH (not supported for now)
            //
            // In this implementation, we construct a chain of partial query tree node, one for each
            // MATCH clause and WITH clause

            SingleQueryNode queryNode = null;
            PartialQueryNode prevQueryPart = null;

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (child is CypherParser.OC_ReadingClauseContext) // MATCH (WHERE)
                {
                    Debug.Assert(queryNode == null);

                    var partQueryTree = CreateQueryPartFromReadingClauses(
                        child as CypherParser.OC_ReadingClauseContext,
                        prevQueryPart
                        );

                    prevQueryPart = partQueryTree;
                }
                else if (child is CypherParser.OC_WithContext) // WITH (WHERE)
                {
                    Debug.Assert(queryNode == null);

                    // when we see an WITH, we know that we we are done with one query part of a multi-query part
                    // we will construct a new query part object, and if applicable, chain with previous query part
                    // and construct the MATCH patterns
                    if (prevQueryPart == null)
                    {
                        throw new TranspilerNotSupportedException("WITH projection without reading clauses (a.k.a. Match)");
                    }

                    var withResult = ((
                        bool IsDistinct,
                        IList<QueryExpressionWithAlias> ReturnItems,
                        QueryExpression Condition,
                        IList<SortItem> OrderByItems,
                        LimitClause Limit
                        ))Visit(child);

                    // WITH will mask out any entities from MATCH that was not explicitly returned
                    // get a list of entities exposed from current query part and leave it for next query part (if any)
                    // for constructing the match pattern that refers to any of these entities
                    // If we have a previous query part, we get a list of variables from previous query that
                    // directly exposes node/relationship entities. E.g.
                    //    MATCH (u:user)   <-- prev query part
                    //    WITH u as u2
                    //    MATCH (u)-[:some_rel]-(:some_node) <-- current query part, u is not the same u in previous MATCH but a new node
                    //    ...
                    var projectionExprs = withResult.ReturnItems ?? 
                        throw new TranspilerSyntaxErrorException($"Expecting a valid list of items to be projected: {child.GetText()}");

                    // similar to that in SingleQuery case, update the entity type for entity aliases
                    // in addition, do that for OrderBy and Where clauses too
                    UpdateEntityTypesInExpressionInPlace(
                        projectionExprs
                            .Union(withResult.Condition != null ?
                                new List<QueryExpression>() {withResult.Condition} :
                                Enumerable.Empty<QueryExpression>())
                            .Union(withResult.OrderByItems != null ?
                                withResult.OrderByItems.Select(o => o.InnerExpression) :
                                Enumerable.Empty<QueryExpression>()),
                        GetDirectlyExposedEntitiesWithAliasApplied(prevQueryPart.ProjectedExpressions)
                        );

                    var partQueryTree = new PartialQueryNode()
                    {
                        PipedData = prevQueryPart,
                        ProjectedExpressions = projectionExprs,
                        FilterExpression = withResult.Condition,
                        IsDistinct = withResult.IsDistinct,
                        MatchData = null, // in this implementation, we do not put MATCH clause and WITH clause into one query part
                        IsImplicitProjection = false, // indication this is a WITH/RETURN with explicitly projection
                        CanChainNonOptionalMatch = true,
                        Limit = withResult.Limit,
                        OrderByClause = withResult.OrderByItems,
                    };
                    prevQueryPart = partQueryTree;
                }
                else if (child is CypherParser.OC_SinglePartQueryContext) // (ReadingClauses)* RETURN
                {
                    Debug.Assert(prevQueryPart != null);

                    queryNode = ConstructSingleQuery(child as CypherParser.OC_SinglePartQueryContext, prevQueryPart);
                    prevQueryPart = null;
                }
                else if (child is CypherParser.OC_UpdatingClauseContext)
                {
                    throw new TranspilerNotSupportedException("Updating clause");
                }
                else
                {
                    // skip the rest type nodes (e.g. SP)
                }
            }

            Debug.Assert(queryNode != null);
            return queryNode;
        }

        public override object VisitOC_SinglePartQuery([NotNull] CypherParser.OC_SinglePartQueryContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_SinglePartQuery", context.GetText());
            // return SingleQueryTreeNode
            // single part query is the terminating part that contains RETURN
            // it is either MATCH RETURN, MATCH MATCH ... RETURN, or just RETURN
            var queryNode = ConstructSingleQuery(context as CypherParser.OC_SinglePartQueryContext, null);
            return queryNode;
        }

        public override object VisitOC_ReadingClause([NotNull] CypherParser.OC_ReadingClauseContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_ReadingClause", context.GetText());
            // reading clause lead to MATCH, which is supported, and Unwind/InQueryCall, which are not supported
            // returns valuetuple type (pass through):
            //   { MatchPatterns(IList<MatchPattern>), Condition(QueryExpression) }

            if (context.oC_Unwind() != null || context.oC_InQueryCall() != null)
            {
                throw new TranspilerNotSupportedException("Unwind or Call");
            }

            if (context.oC_Match() != null)
            {
                // returns valuetuple type (MatchPatterns, Condition) (pass through):
                return Visit(context.oC_Match());
            }
            else
            {
                throw new TranspilerSyntaxErrorException("Match is required");
            }
        }

        public override object VisitOC_Match([NotNull] CypherParser.OC_MatchContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Match", context.GetText());
            // match has MATCH <patterns> (WHERE)
            // returns valuetuple type:
            //   ( MatchPatterns(IList<MatchPattern>), Condition(QueryExpression) )

            var isOptional = IsContextContainsTextToken(context, "OPTIONAL");
            var patterns = Visit(context.oC_Pattern()) as IList<IList<Entity>> ?? throw new TranspilerInternalErrorException("Parsing oc_Pattern");
            QueryExpression condExpr = null;

            if (context.oC_Where() != null)
            {
                condExpr = Visit(context.oC_Where()) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oc_Where");
            }

            return (
                MatchPatterns: patterns.Select(l => new MatchPattern(isOptional, l)).ToList() as IList<MatchPattern>,
                Condition: condExpr // Condition
            );
        }

        public override object VisitOC_Pattern([NotNull] CypherParser.OC_PatternContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Pattern", context.GetText());
            // returns IList<IList<Entity>> type

            var patterns = new List<IList<Entity>>();

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (child is CypherParser.OC_PatternPartContext)
                {
                    var result = Visit(child) as IList<Entity>;
                    Debug.Assert(result != null);
                    patterns.Add(result);
                }
                else
                {
                    // skip the rest type nodes (e.g. SP)
                }
            }
            return patterns;
        }

        public override object VisitOC_PatternPart([NotNull] CypherParser.OC_PatternPartContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_PatternPart", context.GetText());
            // return IList<Entity> (pass through)
            IList<Entity> matchPatternPart = null;

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (child is CypherParser.OC_AnonymousPatternPartContext)
                {
                    var result = Visit(child) as IList<Entity>;
                    Debug.Assert(result != null);
                    Debug.Assert(matchPatternPart == null);
                    matchPatternPart = result;
                }
                else if (child is CypherParser.OC_VariableContext)
                {
                    var text = child.GetText();
                    throw new TranspilerNotSupportedException($"Variable on the match pattern: {text}");
                }
                else
                {
                    // skip the rest type nodes (e.g. SP)
                }
            }
            return matchPatternPart;
        }

        public override object VisitOC_PatternElement([NotNull] CypherParser.OC_PatternElementContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_PatternElement", context.GetText());
            // returns IList<Entity>
            // an pattern element is node - chain or just node, or ( itself )
            var matchPatternPart = new List<Entity>();

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (child is CypherParser.OC_NodePatternContext)
                {
                    var result = Visit(child) as NodeEntity;
                    Debug.Assert(result != null);
                    matchPatternPart.Add(result);
                }
                else if (child is CypherParser.OC_PatternElementChainContext || child is CypherParser.OC_PatternElementContext)
                {
                    var result = Visit(child) as IList<Entity>;
                    Debug.Assert(result != null);
                    Debug.Assert(matchPatternPart.Count % 2 == 1); // number of entity should always be odd, e.g. 1 node, 1 node + 1 rel + 1 node, ...
                    Debug.Assert(result.First() != null && result.First() is RelationshipEntity);
                    (result.First() as RelationshipEntity).LeftEntityName = matchPatternPart.Last().EntityName;
                    matchPatternPart.AddRange(result);
                }
                else
                {
                    // skip the rest type nodes (e.g. SP)
                }
            }
            return matchPatternPart;
        }
        public override object VisitOC_PatternElementChain([NotNull] CypherParser.OC_PatternElementChainContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_PatternElementChain", context.GetText());
            // returns IList<Entity>
            // a chain is the -rel-node part
            RelationshipEntity rel = null;
            NodeEntity sinkNode = null;

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (child is CypherParser.OC_RelationshipPatternContext)
                {
                    var result = Visit(child) as RelationshipEntity;
                    Debug.Assert(rel == null);
                    rel = result;
                }
                else if (child is CypherParser.OC_NodePatternContext)
                {
                    var result = Visit(child) as NodeEntity;
                    Debug.Assert(sinkNode == null);
                    Debug.Assert(rel != null);
                    sinkNode = result;
                    rel.RightEntityName = result.EntityName;
                }
                else
                {
                    // skip the rest type nodes (e.g. SP)
                }
            }

            Debug.Assert(rel != null && sinkNode != null);
            return new List<Entity>() { rel, sinkNode };
        }

        public override object VisitOC_NodePattern([NotNull] CypherParser.OC_NodePatternContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_NodePattern", context.GetText());
            // returns a NodeEntity object

            NodeEntity node = new NodeEntity();

            if (context.oC_NodeLabels() != null)
            {
                var result = (string)Visit(context.oC_NodeLabels());
                Debug.Assert(result != null);
                node.EntityName = result;
            }

            if (context.oC_Properties() != null)
            {
                throw new TranspilerNotSupportedException("Please consider WHERE. Properties set on the node match pattern");
            }

            if (context.oC_Variable() != null)
            {
                // variable for match pattern in node is optional
                var result = (string)Visit(context.oC_Variable());
                Debug.Assert(result != null);
                node.Alias = result;
            }

            return node;
        }

        public override object VisitOC_NodeLabels([NotNull] CypherParser.OC_NodeLabelsContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_NodeLabels", context.GetText());
            string labelName = null;

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (child is CypherParser.OC_NodeLabelContext)
                {
                    var nextLabelName = (child as CypherParser.OC_NodeLabelContext).oC_LabelName().GetText();
                    if (labelName != null)
                    {
                        throw new TranspilerNotSupportedException($"Multiple nodel labels: {labelName} {nextLabelName}");
                    }
                    labelName = nextLabelName;
                }
                else
                {

                }
            }
            return labelName;
        }

        public override object VisitOC_RelationshipPattern([NotNull] CypherParser.OC_RelationshipPatternContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_RelationshipPattern", context.GetText());
            // returns a RelationshipEntity object

            RelationshipEntity relationship = new RelationshipEntity()
            {
                RelationshipDirection =
                    (context.oC_LeftArrowHead() != null ?
                      (context.oC_RightArrowHead() != null ? RelationshipEntity.Direction.Both : RelationshipEntity.Direction.Backward) :
                      (context.oC_RightArrowHead() != null ? RelationshipEntity.Direction.Forward : RelationshipEntity.Direction.Both))
            };

            var result = ((string VarName, string RelName))Visit(context.oC_RelationshipDetail());
            relationship.EntityName = result.RelName;
            relationship.Alias = result.VarName;
            return relationship;
        }

        public override object VisitOC_RelationshipDetail([NotNull] CypherParser.OC_RelationshipDetailContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_RelationshipDetail", context.GetText());
            // returns valuetuple type {VarName(string), RelName(string)}

            string varName = null;
            string relationshipName = null;

            if (context.oC_Variable() != null)
            {
                varName = (string)Visit(context.oC_Variable());
            }

            if (context.oC_RelationshipTypes() != null)
            {
                relationshipName = (string)Visit(context.oC_RelationshipTypes());
            }

            if (context.oC_Properties() != null)
            {
                throw new TranspilerNotSupportedException("Please consider WHERE. Properties set on the relationship match pattern");
            }

            return (VarName: varName, RelName: relationshipName);
        }

        public override object VisitOC_RelationshipTypes([NotNull] CypherParser.OC_RelationshipTypesContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_RelationshipTypes", context.GetText());
            string relationshipName = null;

            // the relationship is the type specified inside [] bracket
            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (child is CypherParser.OC_RelTypeNameContext)
                {
                    var relName = child.GetText();
                    // Does not support multiple re
                    if (relationshipName != null)
                    {
                        throw new TranspilerNotSupportedException($"Multiple relationship: {relationshipName}|{relName}");
                    }
                    relationshipName = relName;
                }
                else
                {
                    // ignore the rest of things
                }
            }
            return relationshipName;
        }

        public override object VisitOC_With([NotNull] CypherParser.OC_WithContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_With", context.GetText());
            // with can come with WITH <expr>, ... or WITH <expr> WHERE <expr>

            var isDistinct = IsContextContainsTextToken(context, "DISTINCT");

            Debug.Assert(context.oC_ReturnBody() != null);
            var returnBodyResult = ((
                IList<QueryExpressionWithAlias> ReturnItems,
                IList<SortItem> OrderByItems,
                LimitClause Limit))Visit(context.oC_ReturnBody());

            var queryExprs = returnBodyResult.ReturnItems ?? throw new TranspilerInternalErrorException("Parsing oC_ReturnBody");

            QueryExpression condExpr = null;
            if (context.oC_Where() != null)
            {
                condExpr = Visit(context.oC_Where()) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_Where");
            }

            return (
                IsDistinct: isDistinct, 
                ReturnItems: queryExprs, 
                Conditions: condExpr, 
                OrderByItems: returnBodyResult.OrderByItems, 
                Limit: returnBodyResult.Limit
                );
        }

        public override object VisitOC_Return([NotNull] CypherParser.OC_ReturnContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Return", context.GetText());

            var returnBody = context.oC_ReturnBody();

            bool isDistinct = IsContextContainsTextToken(context, "DISTINCT");

            var returnBodyResult = ((
                            IList<QueryExpressionWithAlias> ReturnItems,
                            IList<SortItem> OrderByItems,
                            LimitClause Limit))Visit(returnBody);

            return (
                IsDistinct: isDistinct,
                ReturnItems: returnBodyResult.ReturnItems,
                OrderByItems: returnBodyResult.OrderByItems,
                Limit: returnBodyResult.Limit
                );
        }

        public override object VisitOC_ReturnItems([NotNull] CypherParser.OC_ReturnItemsContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_ReturnItems", context.GetText());

            var expressions = new List<QueryExpressionWithAlias>();

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);
                if (child is CypherParser.OC_ReturnItemContext)
                {
                    // child returns expression
                    var result = Visit(child) as QueryExpressionWithAlias ?? throw new TranspilerSyntaxErrorException($"{child.GetText()}. Expecting query expressions.");
                    expressions.Add(result);
                }
            }
            return expressions;
        }

        public override object VisitOC_ReturnItem([NotNull] CypherParser.OC_ReturnItemContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_ReturnItem", context.GetText());

            var selectedProperty = new QueryExpressionWithAlias();
            bool visitedExpr = false;
            bool visitedAlias = false;

            for (int i = 0; i < context.ChildCount; i++)
            {
                if (!visitedExpr && context.GetChild(i) is CypherParser.OC_ExpressionContext)
                {
                    var expr = Visit(context.GetChild(i)) as QueryExpression;
                    Debug.Assert(expr != null);
                    selectedProperty.InnerExpression = expr;
                    visitedExpr = true;
                    continue;
                }

                if (!visitedAlias && context.GetChild(i) is CypherParser.OC_VariableContext)
                {
                    var varName = (string)Visit(context.GetChild(i));
                    selectedProperty.Alias = varName;
                    visitedAlias = true;
                    continue;
                }
            }

            // if no alias is specified, then we infer from the property name / variable itself, or, if ambiguous, throw
            // syntax exception
            if (!visitedAlias)
            {
                var props = selectedProperty.InnerExpression.GetChildrenQueryExpressionType<QueryExpressionProperty>();
                if (props.Count() != 1)
                {
                    throw new TranspilerSyntaxErrorException($"You must specify alias name for expression {context.GetText()}");
                }
                else
                {
                    selectedProperty.Alias = props.First().PropertyName ?? props.First().VariableName;
                }
            }

            Debug.Assert(visitedExpr);
            return selectedProperty;
        }

        public override object VisitOC_Where([NotNull] CypherParser.OC_WhereContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Where", context.GetText());
            // where returns single expression object (pass through)
            return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oc_Expression");
        }

        public override object VisitOC_Properties([NotNull] CypherParser.OC_PropertiesContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Properties", context.GetText());
            // Properties are JSON attached to Node or Edge match patterns. e.g.
            // MATCH (n:device { id: 'someid'})
            // not supported for now
            throw new TranspilerNotSupportedException("Consider use WITH/WHERE. Properties specification");
        }

        public override object VisitOC_Expression([NotNull] CypherParser.OC_ExpressionContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Expression", context.GetText());
            // returns single expression (pass through)
            return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_OrExpression");
        }
        public override object VisitOC_OrExpression([NotNull] CypherParser.OC_OrExpressionContext context)
        {
            // OR is a binary operator, so if we see more than 1 children, the actual statement is seem than a pass-thru
            if ((context.OR()?.Length ?? 0) > 0)
            {
                Debug.Assert(context.oC_XorExpression().Length >= 2);
                _logger?.LogVerbose("{0}: {1}", "VisitOC_OrExpression", context.GetText());
                return HandlesBinaryExpression(context, "OR") ?? throw new TranspilerInternalErrorException("Parsing two oC_XorExpression of OR");
            }
            else
            {
                // pass through. this is not an OR expression
                return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_XorExpression");
            }
        }
        public override object VisitOC_XorExpression([NotNull] CypherParser.OC_XorExpressionContext context)
        {
            // XOR is a binary operator, so if we see more than 1 children, the actual statement is seem than a pass-thru
            if ((context.XOR()?.Length ?? 0) > 0)
            {
                Debug.Assert(context.oC_AndExpression().Length >= 2);
                _logger?.LogVerbose("{0}: {1}", "VisitOC_XorExpression", context.GetText());
                return HandlesBinaryExpression(context, "XOR") ?? throw new TranspilerInternalErrorException("Parsing two oC_AndExpression of XOR");
            }
            else
            {
                // pass through. this is not an XOR expression
                return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_AndExpression");
            }
        }
        public override object VisitOC_AndExpression([NotNull] CypherParser.OC_AndExpressionContext context)
        {
            // AND is a binary operator, so if we see more than 1 children, the actual statement is seem than a pass-thru
            if ((context.AND()?.Length ?? 0) > 0)
            {
                Debug.Assert(context.oC_NotExpression().Length >= 2);
                _logger?.LogVerbose("{0}: {1}", "VisitOC_AndExpression", context.GetText());
                return HandlesBinaryExpression(context, "AND") ?? throw new TranspilerInternalErrorException("Parsing two oC_NotExpression inside AND");
            }
            else
            {
                // pass through. this is not an AND expression
                return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_NotExpression");
            }
        }
        public override object VisitOC_NotExpression([NotNull] CypherParser.OC_NotExpressionContext context)
        {
            if ((context.NOT()?.Length ?? 0) > 0)
            {
                Debug.Assert(context.NOT().Length == 1);
                _logger?.LogVerbose("{0}: {1}", "VisitOC_NotExpression", context.GetText());
                return HandlesUnaryFuncExpression(context, "NOT") ?? throw new TranspilerInternalErrorException("Parsing oC_ComparisonExpression inside NOT");
            }
            else
            {
                // pass through. this is not an Not expression
                return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_ComparisonExpression");
            }
        }
        public override object VisitOC_ComparisonExpression([NotNull] CypherParser.OC_ComparisonExpressionContext context)
        {
            // If we see PartialComparison then it is a real comparison expr, otherwise pass through
            if ((context.oC_PartialComparisonExpression()?.Length ?? 0) > 0)
            {
                _logger?.LogVerbose("{0}: {1}", "VisitOC_ComparisonExpression", context.GetText());
                Debug.Assert(context.oC_PartialComparisonExpression().Length == 1);

                var parentExpression = new QueryExpressionBinary();

                bool visitedLeft = false;
                bool visitedRight = false;

                // get left expression
                for (int i = 0; i < context.ChildCount; i++)
                {
                    var child = context.GetChild(i);

                    if (!visitedLeft && !(child is ITerminalNode))
                    {
                        Debug.Assert(child is CypherParser.OC_AddOrSubtractExpressionContext);

                        var childExpr = Visit(child) as QueryExpression ?? throw new TranspilerSyntaxErrorException($"Expect valid left expression {child.GetText()}");
                        parentExpression.LeftExpression = childExpr;
                        visitedLeft = true;
                        continue;
                    }

                    if (!visitedRight && !(context.GetChild(i) is ITerminalNode))
                    {
                        var result = ((string OP, QueryExpression Expr))Visit(child);
                        var childExpr = result.Expr as QueryExpression ?? throw new TranspilerSyntaxErrorException($"Expect valid right expression {child.GetText()}");
                        var op = (string)result.OP ?? throw new TranspilerSyntaxErrorException($"Expect valid comparison operator {child.GetText()}");
                        parentExpression.RightExpression = childExpr;

                        var opEnum = OperatorHelper.TryGetOperator(op)
                            ?? throw new TranspilerSyntaxErrorException($"Unknown or unsupported operator {op}");
                        parentExpression.Operator = opEnum;
                        visitedRight = true;
                        continue;
                    }
                }

                Debug.Assert(parentExpression.LeftExpression != null && parentExpression.RightExpression != null && parentExpression.Operator != null);
                return parentExpression;
            }
            else
            {
                // pass through. this is not a comparison expression
                return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_AddOrSubtractExpression");
            }
        }

        public override object VisitOC_PartialComparisonExpression([NotNull] CypherParser.OC_PartialComparisonExpressionContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_PartialComparisonExpression", context.GetText());
            // returns valuetuple object that (OP(string), Expr(QueryExpression))

            bool visitedOperator = false;
            bool visitedExpr = false;
            string op = null;
            QueryExpression expr = null;

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);

                if (!visitedOperator && child is ITerminalNode && !IsContextSP(context))
                {
                    op = context.GetChild(i).GetText();
                    visitedOperator = true;
                    continue;
                }

                if (!visitedExpr && !(child is ITerminalNode))
                {
                    expr = Visit(child) as QueryExpression ?? throw new TranspilerSyntaxErrorException($"Expect valid right expression {child.GetText()}");
                    visitedExpr = true;
                    continue;
                }
            }

            Debug.Assert(visitedOperator && visitedExpr);
            return (OP: op, Expr: expr);
        }

        public override object VisitOC_AddOrSubtractExpression([NotNull] CypherParser.OC_AddOrSubtractExpressionContext context)
        {
            // + or - is a binary operator and can chain up, so if we see more than 1 children, the actual statement is seem than a pass-thru
            // MultipleDeviceModulo is immediately nested inside AddOrSub due to operator priority
            if ((context.oC_MultiplyDivideModuloExpression()?.Length ?? 0) > 1)
            {
                _logger?.LogVerbose("{0}: {1}", "VisitOC_AddOrSubtractExpression", context.GetText());
                return HandleChainedBinaryExpressions(context) ?? throw new TranspilerInternalErrorException("Parsing multiple oC_MultiplyDivideModuloExpression inside +/-");
            }
            else
            {
                // pass through. this is not an AddOrSub chain expression
                return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_MultiplyDivideModuloExpression");
            }
        }
        public override object VisitOC_MultiplyDivideModuloExpression([NotNull] CypherParser.OC_MultiplyDivideModuloExpressionContext context)
        {
            if ((context.oC_PowerOfExpression()?.Length ?? 0) > 1)
            {
                _logger?.LogVerbose("{0}: {1}", "VisitOC_MultiplyDivideModuloExpression", context.GetText());
                return HandleChainedBinaryExpressions(context) ?? throw new TranspilerInternalErrorException("Parsing multiple oC_PowerOfExpression inside +/-");
            }
            else
            {
                // pass through. this is not an MultiDeviceMod chain expression
                return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_PowerOfExpression");
            }
        }
        public override object VisitOC_PowerOfExpression([NotNull] CypherParser.OC_PowerOfExpressionContext context)
        {
            if ((context.oC_UnaryAddOrSubtractExpression()?.Length ?? 0) > 1)
            {
                _logger?.LogVerbose("{0}: {1}", "VisitOC_PowerOfExpression", context.GetText());
                return HandlesBinaryExpression(context, "^") ?? throw new TranspilerInternalErrorException("Parsing multiple oC_UnaryAddOrSubtractExpression inside +/-");
            }
            else
            {
                // pass through. this is not a Power chain expression
                return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_UnaryAddOrSubtractExpression");
            }
        }
        public override object VisitOC_UnaryAddOrSubtractExpression([NotNull] CypherParser.OC_UnaryAddOrSubtractExpressionContext context)
        {
            var supportedOperators = new string[] { "+", "-" };
            bool isUnaryAddSub = false;
            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);
                if (child is ITerminalNode && supportedOperators.Contains(child.GetText()))
                {
                    isUnaryAddSub = true;
                    break;
                }
            }

            if (isUnaryAddSub)
            {
                _logger?.LogVerbose("{0}: {1}", "VisitOC_UnaryAddOrSubtractExpression", context.GetText());
                return HandlesUnaryExpression(context) ?? throw new TranspilerInternalErrorException("Parsing oC_StringListNullOperatorExpression inside unary +/-");
            }
            else
            {
                // pass through. this is not an MultiDeviceMod chain expression
                return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_StringListNullOperatorExpression");
            }
        }

        public override object VisitOC_StringListNullOperatorExpression([NotNull] CypherParser.OC_StringListNullOperatorExpressionContext context)
        {
            // Handles PropertyOrLabelsExpression:
            // 
            // oC_PropertyOrLabelsExpression 
            // (
            //  ( SP? '[' oC_Expression ']' ) |    <---- Label list or range is not supported today
            //  ( SP? '[' oC_Expression? '..' oC_Expression? ']' ) |     <---- Label list or range is not supported today
            //  ( ( ( SP IN ) | ( SP STARTS SP WITH ) | ( SP ENDS SP WITH ) | ( SP CONTAINS ) ) SP? oC_PropertyOrLabelsExpression ) |
            //  ( SP IS SP NULL ) | ( SP IS SP NOT SP NULL ) 
            // )*

            if (IsMatchSequence(context, "[", typeof(CypherParser.OC_ExpressionContext), "]") ||
                IsMatchSequence(context, "[", typeof(Optional<CypherParser.OC_ExpressionContext>), "..", typeof(Optional<CypherParser.OC_ExpressionContext>), "]"))
            {
                throw new TranspilerNotSupportedException($"Label list or range: {context.GetText()}");
            }
            else if (context.ChildCount > 1)
            {
                _logger?.LogVerbose("{0}: {1}", "VisitOC_StringListNullOperatorExpression", context.GetText());

                QueryExpression funcOrOp = null;

                // handles string operators
                if ((context.IN()?.Length ?? 0) > 0)
                {
                    // operator type for 'IN'
                    funcOrOp = HandlesBinaryExpression(context, "in") ?? throw new TranspilerInternalErrorException("Parsing IN statement");
                }
                else
                {
                    // other alternatives such as STARTS WITH / ENDS WITH ... , we construct as function types
                    var strFunc = new QueryExpressionFunction();

                    if ((context.STARTS()?.Length ?? 0) > 0 && (context.WITH()?.Length ?? 0) > 0)
                    {
                        strFunc.Function = FunctionHelper.GetFunctionInfo(Function.StringStartsWith);
                    }
                    else if ((context.ENDS()?.Length ?? 0) > 0 && (context.WITH()?.Length ?? 0) > 0)
                    {
                        strFunc.Function = FunctionHelper.GetFunctionInfo(Function.StringEndsWith);
                    }
                    else if ((context.CONTAINS()?.Length ?? 0) > 0)
                    {
                        strFunc.Function = FunctionHelper.GetFunctionInfo(Function.StringContains);
                    }
                    else if ((context.IS()?.Length ?? 0) > 0 && (context.NULL()?.Length ?? 0) > 0 && (context.NOT()?.Length ?? 0) == 0)
                    {
                        // IS NULL
                        strFunc.Function = FunctionHelper.GetFunctionInfo(Function.IsNull);
                    }
                    else if ((context.IS()?.Length ?? 0) > 0 && (context.NULL()?.Length ?? 0) > 0 && (context.NOT()?.Length ?? 0) > 0)
                    {
                        // IS NOT NULL
                        strFunc.Function = FunctionHelper.GetFunctionInfo(Function.IsNotNull);
                    }
                    else
                    {
                        throw new TranspilerNotSupportedException($"String manipulation function/operator used in {context.GetText()}");
                    }

                    if (strFunc.Function.RequiredParameters + strFunc.Function.OptionalParameters > 0)
                    {
                        var parameters = new List<QueryExpression>();

                        for (int i = 0; i < context.ChildCount; i++)
                        {
                            if (!(context.GetChild(i) is ITerminalNode))
                            {
                                parameters.Add(Visit(context.GetChild(i)) as QueryExpression 
                                    ?? throw new TranspilerInternalErrorException($"Parsing oC_PropertyOrLabelsExpression inside '{strFunc.Function.FunctionName}'"));
                            }
                        }

                        if (strFunc.Function.RequiredParameters > parameters.Count)
                        {
                            throw new TranspilerSyntaxErrorException($"Function {strFunc.Function.FunctionName} expects at least {strFunc.Function.RequiredParameters} parameters");
                        }
                        if (strFunc.Function.RequiredParameters + strFunc.Function.OptionalParameters > parameters.Count)
                        {
                            throw new TranspilerSyntaxErrorException($"Function {strFunc.Function.FunctionName} expects at most {strFunc.Function.RequiredParameters + strFunc.Function.OptionalParameters} parameters");
                        }

                        strFunc.InnerExpression = parameters.First();
                        strFunc.AdditionalExpressions = parameters.Skip(1);
                    }

                    funcOrOp = strFunc;
                }

                return funcOrOp;
            }
            else
            {
                // pass through if this is not a stringlistnulloperator expression
                return VisitChildren(context) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_PropertyOrLabelsExpression");
            }
        }
        public override object VisitOC_PropertyOrLabelsExpression([NotNull] CypherParser.OC_PropertyOrLabelsExpressionContext context)
        {
            // this can be a Atom.PropertyLookup or just Atom, where Atom will be some lterial/param or bunch of other operations
            // we will report unhandled case if NodeLabels appears (Atom NodeLabels or Atom.PropertyLookup.NodeLabels)
            // supported Atom can be Literal, Parameter, ParenthesizedExpression, Variable and FunctionInvocationm or COUNT(*)
            // currently, PatternComprehension, CaseExpression, Parameter (as in Printf style of writting OC query and literals)
            _logger?.LogVerbose("{0}: {1}", "VisitOC_PropertyOrLabelsExpression", context.GetText());

            if (context.oC_NodeLabels() != null)
            {
                throw new TranspilerNotSupportedException($"Node labels in the expression: {context.GetText()}");
            }

            if ((context.oC_PropertyLookup()?.Length ?? 0) > 0)
            {
                // this is a Atom.Property look up
                if (context.oC_PropertyLookup().Length > 1)
                {
                    throw new TranspilerNotSupportedException($"Nested property lookup in '{context.GetText()}'");
                }

                // we only support direct reference to variable name for now
                var propertyObj = Visit(context.oC_Atom()) as QueryExpressionProperty ?? throw new TranspilerInternalErrorException("Parsing oC_Atom");
                var propName = context.oC_PropertyLookup().First().oC_PropertyKeyName().GetText() ?? throw new TranspilerInternalErrorException("Parsing oC_PropertyLookup");
                propertyObj.PropertyName = propName;

                return propertyObj;
            }
            else
            {
                // pass through (assuming it is an expression)
                return Visit(context.oC_Atom()) as QueryExpression;
            }
        }
        public override object VisitOC_FunctionInvocation([NotNull] CypherParser.OC_FunctionInvocationContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_FunctionInvocation", context.GetText());

            QueryExpression funcExpr = null;
            bool hasDistinct = IsContextContainsTextToken(context, "DISTINCT");
            var innerExprs = new List<QueryExpression>();

            var funcName = context.oC_FunctionName().GetText();

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);
                if (child is CypherParser.OC_ExpressionContext)
                {
                    var result = Visit(child) as QueryExpression;
                    Debug.Assert(result != null);
                    innerExprs.Add(result);
                }
            }

            Debug.Assert(innerExprs.Count > 0);

            AggregationFunction func;
            if (AggregationFunctionHelper.TryParse(funcName, out func))
            {
                // aggregation function
                funcExpr = new QueryExpressionAggregationFunction()
                {
                    AggregationFunction = func,
                    InnerExpression = innerExprs.First(),
                    IsDistinct = hasDistinct,
                };

                if (innerExprs.Count > 1)
                {
                    throw new TranspilerSyntaxErrorException($"Aggregation function '{func}' with more than 1 parameters");
                }
            }
            else
            {
                // non aggregaton function
                if (hasDistinct)
                {
                    throw new TranspilerSyntaxErrorException($"Function '{funcName}' does not support 'DISTINCT' modifier");
                }

                var funcInfo = FunctionHelper.TryGetFunctionInfo(funcName)
                    ?? throw new TranspilerNotSupportedException($"Function: {funcName}");

                funcExpr = new QueryExpressionFunction()
                {
                    Function = funcInfo,
                    InnerExpression = innerExprs.First(),
                    AdditionalExpressions = innerExprs.Count > 1 ? innerExprs.Skip(1) : null
                };
            }

            return funcExpr;
        }

        public override object VisitOC_CaseExpression([NotNull] CypherParser.OC_CaseExpressionContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_CaseExpression", context.GetText());
            var caseNodeVisited = false;
            var elseNodeVisited = false;
            var childCount = context.ChildCount;
            var caseExpression = new QueryExpressionCaseExpression();
            var caseAlterExprs = new List<QueryExpressionCaseAlternative>();
            if (childCount > 0)
            {
                for (int i = 0; i < childCount; i++)
                {
                    // TODO: Add logic to handle case expresion

                    var child = context.GetChild(i);
                    if (child.GetText().ToUpper() == "CASE")
                    {
                        caseNodeVisited = true;
                    }
                    else if (child.GetText().ToUpper() == "ELSE")
                    {
                        elseNodeVisited = true;
                    }
                    else if (child is CypherParser.OC_ExpressionContext)
                    {
                        if (caseNodeVisited && !elseNodeVisited)
                        {
                            var initialCondition = Visit(child) as QueryExpression;
                            throw new TranspilerNotSupportedException($"Please use Case When <Condition> then <value> format.{context.GetText()} ...");
                        }
                        else if (elseNodeVisited)
                        {
                            var elseCondition = Visit(child) as QueryExpression;
                            caseExpression.ElseExpression = elseCondition;
                        }
                        else
                        {
                            throw new TranspilerInternalErrorException($"Unexpected blob in CASE expression: {child.GetText()}");
                        }
                    }
                    else if (child is CypherParser.OC_CaseAlternativesContext)
                    {
                        caseAlterExprs.AddRange(Visit(child) as List<QueryExpressionCaseAlternative>);
                    }
                    else
                    {
                        // passing through other cases
                    }
                }
            }
            caseExpression.CaseAlternatives = caseAlterExprs;


            return caseExpression;

        }
        public override object VisitOC_CaseAlternatives([NotNull] CypherParser.OC_CaseAlternativesContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_CaseAlternatives", context.GetText());

            var childCount = context.ChildCount;
            var caseAlternatives = new List<QueryExpressionCaseAlternative>();
            var isWhenState = false;
            var isThenState = false;

            if (childCount > 0)
            {
                var caseAlterExpr = new QueryExpressionCaseAlternative();
                for (int i = 0; i < childCount; i++)
                {
                    var child = context.GetChild(i);
                    if (child.GetText().ToUpper() == "WHEN")
                    {
                        isWhenState = true;
                        isThenState = false;
                    }
                    else if (child.GetText().ToUpper() == "THEN")
                    {
                        isWhenState = false;
                        isThenState = true;
                    }
                    else if (child is CypherParser.OC_ExpressionContext)
                    {
                        var expr = Visit(child) as QueryExpression;
                        if (isWhenState && !isThenState)
                        {
                            caseAlterExpr.WhenExpression = expr;
                        }
                        else if (!isWhenState && isThenState)
                        {
                            caseAlterExpr.ThenExpression = expr;
                            caseAlternatives.Add(caseAlterExpr);
                            caseAlterExpr = new QueryExpressionCaseAlternative();
                        }
                    }
                }
            }

            return caseAlternatives;
        }

        public override object VisitOC_Variable([NotNull] CypherParser.OC_VariableContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Variable", context.GetText());
            // directly return text inside as variable name
            return context.GetText();
        }

        public override object VisitOC_SchemaName([NotNull] CypherParser.OC_SchemaNameContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_SchemaName", context.GetText());
            // directly return text inside as schema name
            return context.GetText();
        }

        public override object VisitOC_FunctionName([NotNull] CypherParser.OC_FunctionNameContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_FunctionName", context.GetText());
            // TODO: verify if a function is supported or not
            // directly return text inside as function name
            return context.GetText();
        }

        public override object VisitOC_Atom([NotNull] CypherParser.OC_AtomContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Atom", context.GetText());

            // returns a QueryExpression type

            if (context.oC_Variable() != null)
            {
                // this is variable reference. We treat it as either property or alias
                return new QueryExpressionProperty()
                {
                    VariableName = context.GetText(),
                };
            }
            else if (
                context.oC_Literal() != null ||
                context.oC_FunctionInvocation() != null ||
                context.oC_ParenthesizedExpression() != null ||
                context.oC_CaseExpression() != null

                )
            {
                Debug.Assert(context.ChildCount == 1);
                var result = Visit(context.GetChild(0)) as QueryExpression;
                Debug.Assert(result != null);
                return result;
            }
            else
            {
                // currently not supported:
                //   - oC_ListComprehension
                //   - oC_PatternComprehension
                //   - oC_RelationshipsPattern
                //   - COUNT *
                //   - FILTER / EXTRACT / ALL / ANY / NONE / SINGLE
                throw new TranspilerNotSupportedException($"Expression '{context.GetText()}'");
            }
        }

        public override object VisitOC_ParenthesizedExpression([NotNull] CypherParser.OC_ParenthesizedExpressionContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_ParenthesizedExpression", context.GetText());
            Debug.Assert(context.oC_Expression() != null);
            return Visit(context.oC_Expression()) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_Expression");
        }

        public override object VisitOC_Literal([NotNull] CypherParser.OC_LiteralContext context)
        {
            if (context.StringLiteral() != null)
            {
                _logger?.LogVerbose("{0}: {1}: {2}", "VisitOC_Literal", "STRING", context.GetText());

                // cypher literal has single quote around it, removing it
                var quotedText = context.StringLiteral().GetText();
                var quoteChar = quotedText[0];
                string unescapedRawText;

                if (quotedText.Last() != quoteChar || quotedText.Length < 2)
                {
                    throw new TranspilerSyntaxErrorException($"Unrecognized string literal format: {quotedText}, expecting ' or \".");
                }

                if (quoteChar == '\'')
                {
                    unescapedRawText = quotedText.Substring(1, quotedText.Length - 2).Replace("''", "'").Replace("\\\\", "\\");
                }
                else if (quoteChar == '"')
                {
                    unescapedRawText = quotedText.Substring(1, quotedText.Length - 2).Replace("\"\"", "\"").Replace("\\\\", "\\");
                }
                else
                {
                    throw new TranspilerSyntaxErrorException($"Unrecognized string literal format: {quotedText}, expecting ' or \".");
                }

                return new QueryExpressionValue()
                {
                    Value = unescapedRawText
                };
            }
            else if (context.oC_BooleanLiteral() != null || context.oC_NumberLiteral() != null || context.oC_ListLiteral() != null)
            {
                // pass through for oC_* literals that are supported
                return VisitChildren(context);
            }
            else
            {
                _logger?.LogVerbose("{0}: {1}", "VisitOC_Literal", context.GetText());
                throw new TranspilerNotSupportedException($"Literal: {context.GetText()}");
            }
        }
        public override object VisitOC_BooleanLiteral([NotNull] CypherParser.OC_BooleanLiteralContext context)
        {
            if (context.TRUE() != null || context.FALSE() != null)
            {
                _logger?.LogVerbose("{0}: {1}", "VisitOC_BooleanLiteral", context.GetText());
                var booleanText = context.GetText();
                return new QueryExpressionValue()
                {
                    Value = bool.Parse(booleanText)
                };
            }
            else
            {
                // pass through: it is not boolean literal but some other type of literal
                _logger?.LogVerbose("{0}: {1}", "VisitOC_BooleanLiteral", context.GetText());
                return VisitChildren(context);
            }
        }
        public override object VisitOC_ListLiteral([NotNull] CypherParser.OC_ListLiteralContext context)
        {
            if (context.oC_Expression() != null)
            {
                _logger?.LogVerbose("{0}: {1}", "VisitOC_ListLiteral", context.GetText());
                var childExps = new List<QueryExpression>();
                foreach (var child in context.oC_Expression())
                {
                    var exp = Visit(child) as QueryExpression ?? throw new TranspilerInternalErrorException("Parsing oC_Expression");
                    childExps.Add(exp);
                }
                return new QueryExpressionList()
                {
                    ExpressionList = childExps
                };
            }
            else
            {
                _logger?.LogVerbose("{0}: {1}", "VisitOC_ListLiteral", context.GetText());
                // pass through if current type is not a list literal
                return VisitChildren(context) as QueryExpressionValue;
            }
        }
        public override object VisitOC_NumberLiteral([NotNull] CypherParser.OC_NumberLiteralContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_NumberLiteral", context.GetText());
            // return QueryExpressionLiteral (pass through)
            return VisitChildren(context) as QueryExpressionValue;
        }
        public override object VisitOC_IntegerLiteral([NotNull] CypherParser.OC_IntegerLiteralContext context)
        {
            // one of the Integer rep has to be non-null as guaranteed by the Cypher g4 syntax
            Debug.Assert(context.HexInteger() != null || context.OctalInteger() != null || context.DecimalInteger() != null);

            _logger?.LogVerbose("{0}: {1}", "VisitOC_IntegerLiteral", context.GetText());
            var intText = context.GetChild(0).GetText();
            return ParseIntegerLiteral(intText);
        }
        public override object VisitOC_DoubleLiteral([NotNull] CypherParser.OC_DoubleLiteralContext context)
        {
            // one of the Real rep has to be non-null as guaranteed by the Cypher g4 syntax
            Debug.Assert(context.ExponentDecimalReal() != null || context.RegularDecimalReal() != null);
            _logger?.LogVerbose("{0}: {1}", "VisitOC_DoubleLiteral", context.GetText());
            var numberText = context.GetChild(0).GetText();
            return new QueryExpressionValue()
            {
                // TODO: improve the parsing for real number intead of using the C# built int default parser
                Value = double.Parse(numberText)
            };
        }
        public override object VisitOC_RangeLiteral([NotNull] CypherParser.OC_RangeLiteralContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_RangeLiteral", context.GetText());
            throw new TranspilerNotSupportedException("Range literal");
        }
        public override object VisitOC_MapLiteral([NotNull] CypherParser.OC_MapLiteralContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_MapLiteral", context.GetText());
            throw new TranspilerNotSupportedException("Map literal");
        }
        public override object VisitOC_Order([NotNull] CypherParser.OC_OrderContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Order", context.GetText());

            Debug.Assert(context.oC_SortItem() != null);
            var queryExprsOrderBy = new List<SortItem>();

            for (int i = 0; i < context.ChildCount; i++)
            {
                var child = context.GetChild(i);
                if (child is CypherParser.OC_SortItemContext && child != null)
                {
                    var queryExpr = Visit(child) as SortItem;
                    queryExprsOrderBy.Add(queryExpr);
                }
            }

            return queryExprsOrderBy;
        }
        public override object VisitOC_SortItem([NotNull] CypherParser.OC_SortItemContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_SortItem", context.GetText());

            var expr = Visit(context.oC_Expression()) as QueryExpression;
            bool isDescending = IsContextContainsTextToken(context, "DESC") || IsContextContainsTextToken(context, "DESCENDING");
            return new SortItem()
            {
                IsDescending = isDescending,
                InnerExpression = expr
            };
        }
        public override object VisitOC_Limit([NotNull] CypherParser.OC_LimitContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_Limit", context.GetText());
            var exprLimit = new LimitClause();
            if (context.oC_Expression() != null)
            {
                var expr = Visit(context.oC_Expression()) as QueryExpression;
                exprLimit.RowCount = expr.GetChildrenQueryExpressionType<QueryExpressionValue>().First().IntValue;
            }
            return exprLimit;
        }
        public override object VisitOC_ReturnBody([NotNull] CypherParser.OC_ReturnBodyContext context)
        {
            _logger?.LogVerbose("{0}: {1}", "VisitOC_ReturnBody", context.GetText());

            var projectionExprList = new List<QueryExpressionWithAlias>();
            IList<SortItem> sortItems = null;
            LimitClause limit = null;

            // Get returned items: at least one RETURN should've enforced by the generated parser
            Debug.Assert(context.oC_ReturnItems() != null);
            var aliasResult = Visit(context.oC_ReturnItems()) as IList<QueryExpressionWithAlias>;
            projectionExprList.AddRange(aliasResult);

            // Optional OrderBy clause
            if (context.oC_Order() != null)
            {
                sortItems = Visit(context.oC_Order()) as IList<SortItem> ?? throw new TranspilerInternalErrorException("Parsing oC_Order");
            }
            
            // Optional limit clause
            if (context.oC_Limit() != null)
            {
                limit = Visit(context.oC_Limit()) as LimitClause ?? throw new TranspilerInternalErrorException("Parsing oC_Limit");
            }

            return (
                ReturnItems: projectionExprList as IList<QueryExpressionWithAlias>,
                OrderByItems: sortItems,
                Limit: limit
                );
        }

    }

}
