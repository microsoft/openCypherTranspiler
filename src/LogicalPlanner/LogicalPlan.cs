/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/


using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using openCypherTranspiler.Common.Exceptions;
using openCypherTranspiler.Common.GraphSchema;
using openCypherTranspiler.Common.Logging;
using openCypherTranspiler.Common.Utils;
using openCypherTranspiler.openCypherParser.AST;
using openCypherTranspiler.LogicalPlanner.Utils;

namespace openCypherTranspiler.LogicalPlanner
{
    /// <summary>
    /// This class turns abstract syntax tree into logical plan for
    /// further transpiling (rendering) into a relational query language
    /// </summary>
    public class LogicalPlan
    {
        private ILoggable _logger;

        public LogicalPlan(ILoggable logger = null)
        {
            _logger = logger;
        }

        /// <summary>
        /// return a set of operators that are starting points of the logical plan
        /// </summary>
        public IEnumerable<StartLogicalOperator> StartingOperators { get; private set; }

        /// <summary>
        /// return a set of operators that are terminal of the logical plan (representing the output)
        /// </summary>
        public IEnumerable<LogicalOperator> TerminalOperators { get; private set; }
        
        /// <summary>
        /// Create an instance of LogicalPlanner with a given query AST tree
        /// </summary>
        /// <param name="treeRoot"></param>
        /// <param name="graphDef"></param>
        /// <returns></returns>
        public static LogicalPlan ProcessQueryTree(QueryNode treeRoot, IGraphSchemaProvider graphDef, ILoggable logger = null)
        {
            var logicalPlanner = new LogicalPlan(logger);
            var allLogicalOps = new HashSet<LogicalOperator>();

            // from AST, create the logical plan
            var logicalRoot = logicalPlanner.CreateLogicalTree(treeRoot, allLogicalOps);
            logicalPlanner.StartingOperators = logicalRoot.GetAllUpstreamOperatorsOfType<StartLogicalOperator>().ToList();
            logicalPlanner.TerminalOperators = new List<LogicalOperator>(1) { logicalRoot };

            // populate the ids for operators (for debugging purpose)
            var i = 0;
            foreach (var op in allLogicalOps)
            {
                op.OperatorDebugId = ++i;
            }

            // bind logical tree with the graph schema
            var bindableStartingOp = logicalPlanner.StartingOperators.Where(op => op is IBindable);
            bindableStartingOp.ToList().ForEach(op => (op as IBindable).Bind(graphDef));

            // propagate down the schema from starting operator and data types (top down)
            logicalPlanner.PropagateDataTypes();

            // expand and optimize out columns not used (bottom up)
            logicalPlanner.UpdateActualFieldReferencesForEntityFields();

            
            var allOpsFromTraversal = logicalPlanner.StartingOperators.SelectMany(op => op.GetAllDownstreamOperatorsOfType<LogicalOperator>()).Distinct().OrderBy(op => op.OperatorDebugId).ToList();
            var allOpsFromBuildingTree = allLogicalOps.OrderBy(op => op.OperatorDebugId).ToList();
            Debug.Assert(allOpsFromTraversal.Count == allOpsFromBuildingTree.Count);

            return logicalPlanner;
        }

        /// <summary>
        /// Dump textual format of the logical plan
        /// </summary>
        /// <returns></returns>
        public virtual string DumpGraph()
        {
            var sb = new StringBuilder();
            var allOperators = StartingOperators
                .SelectMany(op => op.GetAllDownstreamOperatorsOfType<LogicalOperator>())
                .Distinct()
                .GroupBy(op => op.Depth)
                .OrderBy(g => g.Key);

            foreach (var opGrp in allOperators)
            {
                sb.AppendLine($"Level {opGrp.Key}:");
                sb.AppendLine("--------------------------------------------------------------------------");
                foreach (var op in opGrp)
                {
                    sb.AppendLine($"OpId={op.OperatorDebugId} Op={op.GetType().Name}; InOpIds={string.Join(",", op.InOperators.Select(n => n.OperatorDebugId))}; OutOpIds={string.Join(",", op.OutOperators.Select(n => n.OperatorDebugId))};");
                    sb.AppendLine(op.ToString().ChangeIndentation(1));
                    sb.AppendLine("*");
                }
                sb.AppendLine("--------------------------------------------------------------------------");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Create logical operator tree for a given query AST tree
        /// </summary>
        /// <param name="treeNode"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(QueryNode treeNode, ISet<LogicalOperator> allLogicalOps)
        {
            // Example of logical tree to be created:
            //   MATCH (a:n1)-[b:r1]-(c:n2)
            //   WITH a as c, c as a
            //   OPTIONAL MATCH (c)-[:r2]-(a)
            //   WHERE a.id = "some id"
            //   RETURN a.id as Id
            //
            // The logical tree looks like:
            //    DS(a:n1)  DS(b:r1)   DS(c:n2)
            //         \      /          /
            //          \    /          /
            //      InJoin(a:n1, b:r1) /
            //                \       /
            //                 \     /
            //               InJoin(InJoin(...), c:n2)
            //                         |
            //                Projection(a->c, c->a)   DS(:r2)
            //                               \           /
            //                                \         /
            //                              LfJoin(Proj, :r2)
            //                                     |
            //                               Sel(a.id = 'some id')
            //                                     |
            //                            Projection(a.id -> Id)


            // in-order walk of the tree to evaluate individual queries
            if (treeNode is SingleQueryNode)
            {
                // single query
                return CreateLogicalTree(treeNode as SingleQueryNode, allLogicalOps);
            }
            else
            {
                Debug.Assert(treeNode is InfixQueryNode);
                return CreateLogicalTree(treeNode as InfixQueryNode, allLogicalOps);
            }
        }

        /// <summary>
        /// Create logical operator tree for sub-tree node type InfixQueryNode
        /// </summary>
        /// <param name="treeNode"></param>
        /// <returns></returns>
        private BinaryLogicalOperator CreateLogicalTree(InfixQueryNode treeNode, ISet<LogicalOperator> allLogicalOps)
        {
            var left = CreateLogicalTree(treeNode.LeftQueryTreeNode, allLogicalOps);
            var right = CreateLogicalTree(treeNode.RightQueryTreeNode, allLogicalOps);

            // TODO: better way of ensure schema is the same
            Debug.Assert(left.OutputSchema.Count == left.OutputSchema.Count);

            SetOperator.SetOperationType op;
            switch (treeNode.Operator)
            {
                case InfixQueryNode.QueryOperator.Union:
                    op = SetOperator.SetOperationType.Union;
                    break;
                case InfixQueryNode.QueryOperator.UnionAll:
                    op = SetOperator.SetOperationType.UnionAll;
                    break;
                default:
                    throw new TranspilerInternalErrorException($"Unimplemented operator type: {treeNode.Operator}");
            }

            var setOp = new SetOperator(left, right, op);
            setOp.InputSchema = new Schema(left.InputSchema);
            setOp.OutputSchema = new Schema(left.InputSchema); // the schema remains the same for setoperator
            allLogicalOps.Add(setOp);

            return setOp;
        }

        /// <summary>
        /// Create logical operator tree for projection expressions with optional orderby/limit/filtering
        /// </summary>
        /// <param name="pipedData"></param>
        /// <param name="isDistinct"></param>
        /// <param name="projExprs"></param>
        /// <param name="limitClause"></param>
        /// <param name="orderByClause"></param>
        /// <param name="filterExpr"></param>
        /// <param name="allLogicalOps"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(LogicalOperator pipedData, bool isDistinct, 
            IList<QueryExpressionWithAlias> projExprs, 
            IList<QueryExpressionWithAlias> implicitProjExprs,
            LimitClause limitClause, IList<SortItem> orderByClause, QueryExpression filterExpr, 
            ISet<LogicalOperator> allLogicalOps)
        {
            LogicalOperator lastOperator = pipedData;
            var hasAggregationExpression = projExprs
                .Any(n => n.GetChildrenQueryExpressionType<QueryExpressionAggregationFunction>().Count() > 0);
            var hasOrderByLimit = (limitClause != null || (orderByClause?.Count() ?? 0) > 0);
            var hasWhere = (filterExpr != null);
            var removeExtraImplicitFields = false;

            //  If there is "DISTINCT" keyword, it is equivalent to have an extra "WITH" clause where only explicitly projected
            //  variables and entities are kept valid for downstream ORDER BY, WHERE clauses.
            //
            //  Example: 
            //  MATCH (a)-[]->(b)
            //  WITH DISTINCT a.Name as Name, b.Title as Title
            //  ORDER BY Name, Title
            //  WHERE Name <> "Tom"
            //  RETURN Name, Title
            //  ** ORDER BY/WHERE can only refer Name, Title as there is a DISTINCT
            //
            //  if no distinct 
            //  MATCH (a)-[]->(b)
            //  WITH a.Name as Name, b.Title as Title
            //  ORDER BY a.Tagline, n.Price
            //  WHERE Name <> "Tom"
            //  RETURN Name, Title
            //  ** ORDER BY/WHERE can refer any field in a and b even though only Name and Title
            //     were explicitly projected
            //

            var extraImplicitProjections = implicitProjExprs?.Where(n => projExprs.All(p => p.Alias != n.Alias));

            if ((hasOrderByLimit || hasWhere) &&
                !(isDistinct || hasAggregationExpression) &&
                (extraImplicitProjections?.Count() ?? 0) > 0)
            {
                // If we need to expose the implicit projected entities from previous query parts,
                // and the circumstance allows it (i.e. when DISTINCT/aggregation function was not used)
                // we add extra implicit fields, and will remove it after ORDER BY/WHERE is processed
                var extendedProjectionExprs = projExprs.Union(extraImplicitProjections).ToList();
                lastOperator = CreateLogicalTree(extendedProjectionExprs, isDistinct /*false*/, lastOperator, allLogicalOps);
                removeExtraImplicitFields = true;
            }
            else
            {
                lastOperator = CreateLogicalTree(projExprs, isDistinct, lastOperator, allLogicalOps);
            }

            // Optional OrderBy / Limit
            if (hasOrderByLimit)
            {
                lastOperator = CreateLogicalTree(orderByClause, limitClause, lastOperator, allLogicalOps);
            }

            // Optional WHERE
            if (filterExpr != null)
            {
                // without match, we can just apply filtering if needed (e.g. WITH ... WHERE ...)
                lastOperator = CreateLogicalTree(filterExpr, lastOperator, allLogicalOps);
            }

            // do a simple projection to remove extra implicitly projected fields if applies
            if (removeExtraImplicitFields)
            {
                // by now, all fields need to be projected are already computed in the previous projection operator
                // so we just do a project that retains all the fields that are explicitly projected (by doing an 'field AS field')
                var trimmedSimpleProjExprs = projExprs.Select(n =>
                    new QueryExpressionWithAlias
                    {
                        Alias = n.Alias,
                        InnerExpression = new QueryExpressionProperty()
                        {
                            VariableName = n.Alias,
                            Entity = n.InnerExpression.TryGetDirectlyExposedEntity(),
                        }
                    }
                ).ToList();
                lastOperator = CreateLogicalTree(trimmedSimpleProjExprs, false /*distinct should've applied no not needed here*/, lastOperator, allLogicalOps);
            }

            return lastOperator;
        }

        /// <summary>
        /// Create logical operator for AST node type SingleQueryNode
        /// </summary>
        /// <param name="treeNode"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(SingleQueryNode treeNode, ISet<LogicalOperator> allLogicalOps)
        {
            // Start from the leaf node query part to create logical plan for the single query
            var stack = new Stack<PartialQueryNode>();
            PartialQueryNode lastVisited = treeNode.PipedData;
            while (lastVisited != null)
            {
                stack.Push(lastVisited);
                lastVisited = lastVisited.PipedData;
            }

            LogicalOperator lastOperator = null;
            PartialQueryNode lastNode = null;
            while (stack.Any())
            {
                var curNode = stack.Pop();
                lastOperator = CreateLogicalTree(curNode, lastOperator, allLogicalOps, lastNode);
                lastNode = curNode;
            }

            // Parser should guarantee the case we have at least one clause before RETURN
            Debug.Assert(lastOperator != null);
            // Parser should catch the case that at least one projected field
            Debug.Assert((treeNode.EntityPropertySelected?.Count ?? 0) > 0);

            var finalProjectedExprs = treeNode.EntityPropertySelected;

            // Special to RETURN clause: block any attempt to return entities, as none of our downstream renderer
            // support it right now
            if (finalProjectedExprs.Any(p => p.TryGetDirectlyExposedEntity() != null))
            {
                throw new TranspilerNotSupportedException
                    ($"Please returning fields only. Returning node or relationship entity {treeNode.EntityPropertySelected.First(p => p.TryGetDirectlyExposedEntity() != null).Alias}");
            }

            lastOperator = CreateLogicalTree(
                    lastOperator,
                    treeNode.IsDistinct,
                    finalProjectedExprs,
                    lastNode?.ProjectedExpressions,
                    treeNode.Limit,
                    treeNode.OrderByClause,
                    null, // RETURN has no WHERE conditions
                    allLogicalOps
                    );

            return lastOperator;
        }

        /// <summary>
        /// Create logical operators for a partial query part
        /// </summary>
        /// <param name="treeNode">the current query part</param>
        /// <param name="pipedData"></param>
        /// <param name="allLogicalOps"></param>
        /// <param name="previousNode">the query part immediate before the current query part (treeNode) 
        ///     that may contains expressions that can be accessed by clauses in this query part</param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(PartialQueryNode treeNode, LogicalOperator pipedData, ISet<LogicalOperator> allLogicalOps, PartialQueryNode previousNode)
        {
            LogicalOperator lastOperator = pipedData;

            // parser tree does not mix MATCH and WITH into a single query part today
            Debug.Assert(treeNode.MatchData == null || treeNode.IsImplicitProjection == true);

            if (treeNode.MatchData != null)
            {
                // the query part contains a MATCH clause
                // since the parser tree guarantees no explicit projection (by WITH/RETURN), we do not need to handle things like 
                // DISTINCT or ORDER BY as they appears only with WITH/RETURN
                // we do, however, need to handle WHERE

                var isOptionalMatch = treeNode.MatchData.MatchPatterns.Any(p => p.IsOptionalMatch);
                Debug.Assert(treeNode.MatchData.MatchPatterns.All(p => p.IsOptionalMatch == isOptionalMatch));

                if (treeNode.FilterExpression != null)
                {
                    // with WHERE conditions, optional match requires a fork to apply filtering first and then rejoin with piped data, 
                    // where non-optional doesn't require it
                    // e.g. MATCH ... OPTIONAL MATCH ... WHERE ... WITH/RETURN ...
                    if (isOptionalMatch)
                    {
                        // apply filtering conditions to the optional match pattern ...
                        var modifiedMatch = new MatchClause()
                        {
                            MatchPatterns = treeNode.MatchData.MatchPatterns.Select(p => new MatchPattern(false, p)).ToList()
                        };
                        var omOperator = CreateLogicalTree(modifiedMatch, lastOperator, allLogicalOps);
                        var condOmOperator = CreateLogicalTree(treeNode.FilterExpression, omOperator, allLogicalOps);

                        var leftEntityAliases = lastOperator.OutputSchema
                            .Where(f => f is EntityField).Select(f => (f as EntityField).FieldAlias);
                        var explicitlyMatchedAliasesToJoinWithLeft = modifiedMatch.AllEntitiesOrdered
                            .Select(e => e.Alias)
                            .Where(a => a != null)
                            .Intersect(leftEntityAliases);
                        // ... then left outter join back with the piped data
                        lastOperator = CreateLogicalTree(lastOperator, condOmOperator, explicitlyMatchedAliasesToJoinWithLeft, true, allLogicalOps);
                    }
                    else
                    {
                        // inner join with match data
                        lastOperator = CreateLogicalTree(treeNode.MatchData, lastOperator, allLogicalOps);
                        // apply filtering
                        lastOperator = CreateLogicalTree(treeNode.FilterExpression, lastOperator, allLogicalOps);
                    }
                }
                else
                {
                    // without filtering conditions, optional and non-optional match can join directly, and 
                    // we only need to apply different join type
                    // e.g. MATCH ... OPTIONAL MATCH ... WITH/RETURN ...
                    lastOperator = CreateLogicalTree(treeNode.MatchData, lastOperator, allLogicalOps);
                }

                // apply the implicit project, where the expressions are carried fields plus named entities
                lastOperator = CreateLogicalTree(treeNode.ProjectedExpressions, treeNode.IsDistinct, lastOperator, allLogicalOps);
            }
            else
            {
                // pure projection (WITH/RETURN)
                // we will need to handle ORDER BY/LIMIT

                lastOperator = CreateLogicalTree(
                    lastOperator,
                    treeNode.IsDistinct,
                    treeNode.ProjectedExpressions,
                    previousNode?.ProjectedExpressions,
                    treeNode.Limit,
                    treeNode.OrderByClause,
                    treeNode.FilterExpression,
                    allLogicalOps
                    );
            }
            return lastOperator;
        }

        /// <summary>
        /// Create filtering operator for query expressions
        /// </summary>
        /// <param name="exp"></param>
        /// <param name="pipedData"></param>
        /// <param name="allLogicalOps"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(QueryExpression exp, LogicalOperator pipedData, ISet<LogicalOperator> allLogicalOps)
        {
            var selectionOp = new SelectionOperator(pipedData, exp); 
            selectionOp.InputSchema = new Schema(pipedData.OutputSchema);
            selectionOp.OutputSchema = new Schema(pipedData.OutputSchema); // the schema remains the same for SelectionOperator
            allLogicalOps.Add(selectionOp);
            return selectionOp;
        }

        /// <summary>
        /// Creating a selection operator for ORDER BY ... LIMIT  clause
        /// </summary>
        /// <param name="orderClause">optional orderBy clause with a list of expressions</param>
        /// <param name="limitExp">optional TOP N limit</param>
        /// <param name="pipedData"></param>
        /// <param name="allLogicalOps"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(
            IList<SortItem> orderClause, 
            LimitClause limitExp,
            LogicalOperator pipedData, 
            ISet<LogicalOperator> allLogicalOps)
        {
            var selectionOp = new SelectionOperator(pipedData, orderClause, limitExp);
            var selectionSchema = new Schema(pipedData.OutputSchema);

            // append new property from ORDER BY clause to the the input and output schema
            
            selectionOp.InputSchema = new Schema(selectionSchema);
            selectionOp.OutputSchema = new Schema(selectionSchema); 
            allLogicalOps.Add(selectionOp);
            return selectionOp;
        }
        
        /// <summary>
        /// Create a selection operator for unexpanded inequality expression
        /// </summary>
        /// <param name="conds">a list of inequality conditions, each involes 2 relationship aliases of same relationship type</param>
        /// <param name="pipedData"></param>
        /// <param name="allLogicalOps"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(IEnumerable<(string RelAlias1, string RelAlias2)> conds, LogicalOperator pipedData, ISet<LogicalOperator> allLogicalOps)
        {
            var selectionOp = new SelectionOperator(pipedData, conds);
            selectionOp.InputSchema = new Schema(pipedData.OutputSchema);
            selectionOp.OutputSchema = new Schema(pipedData.OutputSchema); // the schema remains the same for SelectionOperator
            allLogicalOps.Add(selectionOp);
            return selectionOp;
        }

        /// <summary>
        /// Create projection operator
        /// </summary>
        /// <param name="projExprs"></param>
        /// <param name="isDistinct"></param>
        /// <param name="pipedData"></param>
        /// <param name="allLogicalOps"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(IList<QueryExpressionWithAlias> projExprs, bool isDistinct, LogicalOperator pipedData, ISet<LogicalOperator> allLogicalOps)
        {
            // Do a projection
            var outputSchemaFields = pipedData.OutputSchema.OrderBy(f => f.FieldAlias);
            var expectedOutputSchemaFields = projExprs.Select(expr =>
            {
                var innerExpr = expr.InnerExpression;
                var entity = innerExpr.TryGetDirectlyExposedEntity();
                if (entity != null)
                {
                    return new EntityField(
                        expr.Alias,
                        entity.EntityName,
                        entity is NodeEntity ? EntityField.EntityType.Node : EntityField.EntityType.Relationship
                        ) as Field;
                }
                else
                {
                    return new ValueField(
                        expr.Alias
                    ) as Field;
                }
            }).ToList();

            // TODO:
            // verify that we are able to honor the projection by comparing In and OutFields

            // construct the projection logical operator
            var inSchema = new Schema(pipedData.OutputSchema.Select(f => f.Clone()));
            var outSchema = new Schema(expectedOutputSchemaFields);
            var projOperator = new ProjectionOperator(
                pipedData,
                projExprs.ToDictionary(exp => exp.Alias, exp => exp.InnerExpression),
                isDistinct
                );
            projOperator.InputSchema = inSchema;
            projOperator.OutputSchema = outSchema;
            allLogicalOps.Add(projOperator);

            return projOperator;
        }

        /// <summary>
        /// Create join operator to join two set of operators with a list of entity aliases of instances of node which their id should be used in equijoin
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="entityAliasesToJoin">the list of aliases (in the output of left/right) that represents entity instances and should be used in node id equijoin</param>
        /// <param name="isLeftOutterJoin">if false, the join type assumed to be would be inner instead</param>
        /// <param name="allLogicalOps"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(LogicalOperator left, LogicalOperator right, IEnumerable<string> entityAliasesToJoin, bool isLeftOutterJoin, ISet<LogicalOperator> allLogicalOps)
        {
            var joinPairs = entityAliasesToJoin
                .Select(a => new JoinOperator.JoinKeyPair()
                {
                    NodeAlias = a,
                    RelationshipOrNodeAlias = a,
                    Type = JoinOperator.JoinKeyPair.JoinKeyPairType.NodeId
                })
                .ToList();

            if (joinPairs.Count > 0)
            {
                // left outter join or inner join (depending on the parameter)
                var newOp = new JoinOperator(left, right, isLeftOutterJoin ? JoinOperator.JoinType.Left : JoinOperator.JoinType.Inner);
                var fields = left.OutputSchema.Union(right.OutputSchema)
                        .Select(f => f.Clone())
                        .GroupBy(f => f.FieldAlias)
                        .Select(f => f.First());
                newOp.InputSchema = new Schema(fields);
                newOp.OutputSchema = new Schema(fields);
                joinPairs.ForEach(jp => newOp.AddJoinPair(jp));
                allLogicalOps.Add(newOp);
                return newOp;
            }
            else
            {
                // cross join
                var newOp = new JoinOperator(left, right, JoinOperator.JoinType.Cross);
                var fields = left.OutputSchema.Union(right.OutputSchema)
                        .Select(f => f.Clone())
                        .GroupBy(f => f.FieldAlias)
                        .Select(f => f.First());
                newOp.InputSchema = new Schema(fields);
                newOp.OutputSchema = new Schema(fields);
                allLogicalOps.Add(newOp);
                return newOp;
            }
        }

        /// <summary>
        /// This function takes 2 entities and returns in a determinstic order of their aliases:
        /// It guarantees that the order is that if node is the src of edge, then [node, edge]
        /// otherwise [edge, node]
        /// </summary>
        /// <param name="prevEnt"></param>
        /// <param name="ent"></param>
        /// <returns></returns>
        private JoinOperator.JoinKeyPair GetJoinKeyPairInProperOrder(Entity prevEnt, Entity ent)
        {
            Debug.Assert(prevEnt.GetType() != ent.GetType()); // must be one node and one relationship
            var relEntity = (prevEnt is RelationshipEntity ? prevEnt : ent) as RelationshipEntity;
            var nodeEntity = (prevEnt is RelationshipEntity ? ent : prevEnt) as NodeEntity;
            JoinOperator.JoinKeyPair.JoinKeyPairType joinKeyType;

            if (relEntity.RelationshipDirection == RelationshipEntity.Direction.Both)
            {
                if (relEntity.LeftEntityName == relEntity.RightEntityName)
                {
                    // TODO: Blocked for now to force directional traversal. To support this in future,
                    //       We can support this and set the joinKeyType to 'Both' and add support in code
                    //       renderer that support unioning relationships (in this case union with itself by 
                    //       src/dest key reversed)
                    throw new TranspilerNotSupportedException("Consider specifying the direction of traversal <-[]- or -[]->. Directionless traversal for relationship with same type of source and sink entity");
                }
                joinKeyType = JoinOperator.JoinKeyPair.JoinKeyPairType.Either;
            }
            else
            {
                if (prevEnt == nodeEntity)
                {
                    joinKeyType = relEntity.RelationshipDirection == RelationshipEntity.Direction.Forward ?
                        JoinOperator.JoinKeyPair.JoinKeyPairType.Source :
                        JoinOperator.JoinKeyPair.JoinKeyPairType.Sink;
                }
                else
                {
                    joinKeyType = relEntity.RelationshipDirection == RelationshipEntity.Direction.Forward ?
                        JoinOperator.JoinKeyPair.JoinKeyPairType.Sink :
                        JoinOperator.JoinKeyPair.JoinKeyPairType.Source;
                }
            }

            return new JoinOperator.JoinKeyPair()
            {
                NodeAlias = nodeEntity.Alias,
                RelationshipOrNodeAlias = relEntity.Alias,
                Type = joinKeyType
            };
        }

        /// <summary>
        /// Create logical operators for MatchDataSource
        /// </summary>
        /// <param name="matchData"></param>
        /// <param name="pipedData"></param>
        /// <param name="allLogicalOps"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(MatchClause matchData, LogicalOperator pipedData, ISet<LogicalOperator> allLogicalOps)
        {
            // turn linear match patterns into logical operators

            var boolComparer = Comparer<bool>.Create((b1, b2) => b1 ? (b2 ? 0 : 1) : (!b2 ? 0 : -1)); // comparer is true > false
            var anonVarPrefix = "__unnamed_";

            // first build a table of alias to entity, and make sure that every entity
            // reference has a previously given or assigned alias
            var namedMatchData = matchData.AssignGeneratedEntityAliasToAnonEntities(anonVarPrefix);
            var aliasGroups = namedMatchData
                .AllEntitiesOrdered
                .Select((p, i) => new { Order = i, Entity = p })
                .GroupBy(p => p.Entity.Alias);
            var firstDupRelAlias = aliasGroups
                .Where(g => g.Any(e => e.Entity is RelationshipEntity))
                .FirstOrDefault(g => g.Count() > 1);
            if (firstDupRelAlias != null)
            {
                // relationship label should not be repeated in the same MATCH clause
                throw new TranspilerNotSupportedException($"Cannot use the same relationship variable '{firstDupRelAlias.Key}' for multiple patterns");
            }
            var aliasTable = aliasGroups
                .Select(p =>
                {
                    // Assert that we only have one type of entity per alias
                    Debug.Assert(p.Select(e => e.Entity.EntityName).Distinct().Count() == 1);
                    Debug.Assert(p.Select(e => e.GetType()).Distinct().Count() == 1);
                    return p.First();
                })
                .OrderBy(p => p.Order)
                .Select((p, i) => new { Order = i, Entity = p.Entity })
                .ToDictionary(kv => kv.Entity.Alias, kv => kv);

            // build an adjacency matrix represent the DAG for the join logical operator each alias in the form of an array[Dim1, Dim2]
            // note that the list of DataSourceOperator are added to allLogicalOps later in the code after reconciliating with inherited entities
            var logicOpSrcTable = aliasTable.ToDictionary(kv => kv.Key, kv => new DataSourceOperator(kv.Value.Entity) as LogicalOperator);

            // build adjacency matrix with element being the join type between the two entities
            //               alias1, alias2, alias3, ... (Dim 1)
            //       alias1  NA      INNER   LEFT
            //       alias2          NA      LEFT
            //       alias3                  NA
            //       ...
            //       (Dim 2)
            var entityJoinMatrix = new JoinOperator.JoinType[logicOpSrcTable.Count, logicOpSrcTable.Count];

            // build join operator matrix to record the state if logical join operator already created or not
            // between two alias (this will be used later to construct the logical join op creation)
            var hasJoined = new bool[logicOpSrcTable.Count, logicOpSrcTable.Count];

            for (int k = 0; k < entityJoinMatrix.GetLength(0); k++)
            {
                for (int l = 0; l < entityJoinMatrix.GetLength(1); l++)
                {
                    entityJoinMatrix[k, l] = (k == l ? JoinOperator.JoinType.Inner : JoinOperator.JoinType.Cross);
                    hasJoined[k, l] = (k == l ? true : false);
                }
            }

            // if some of the entity referenced are inherited from previous query, such as following overlapping case
            // where some entities returned from previous query parts are referenced in current MATCH patterns
            //    MATCH (a) 
            //    WITH a as b 
            //    MATCH (b)-[c]->(d) ...
            // we will
            //    - update the logical operator table to replace the DataSource with the inherited logical operator instead of
            //      the default DS(n)
            //    - update hasJoined table to indicate those entities piped in were already in the inherited logical operater
            // then we will get the transitive closure of the hasJoined table to show what are the group of entities that
            // are linked together
            var additionalCrossJoinOp = new List<LogicalOperator>();
            var inheritedEntityAliases = pipedData?.OutputSchema
                    .Where(f => f is EntityField).Select(f => (f as EntityField).FieldAlias)
                    .Intersect(aliasTable.Keys).ToList();
            if (pipedData != null)
            {
                if (inheritedEntityAliases.Count > 0)
                {
                    string prevEntAlias = inheritedEntityAliases.First();
                    logicOpSrcTable[prevEntAlias] = pipedData;

                    foreach (var entAlias in inheritedEntityAliases.Skip(1))
                    {
                        logicOpSrcTable[entAlias] = pipedData;

                        var prevEntityAliasIdx = aliasTable[prevEntAlias].Order;
                        var entityAliasIdx = aliasTable[entAlias].Order;
                        hasJoined[prevEntityAliasIdx, entityAliasIdx] = true;
                        hasJoined[entityAliasIdx, prevEntityAliasIdx] = true;   
                    }
                }
                else
                {
                    // there's no overlapping, the piped data will be cross joined
                    additionalCrossJoinOp.Add(pipedData);
                }   
            }
            hasJoined = hasJoined.TransitiveClosure(boolComparer);
            logicOpSrcTable.Values.Where(op => op is DataSourceOperator).Cast<DataSourceOperator>().ToList()
                .ForEach(op =>
                    {
                        op.OutputSchema = new Schema
                        (
                            new List<Field>(1)
                            {
                                new EntityField(
                                    op.Entity.Alias,
                                    op.Entity.EntityName,
                                    op.Entity is NodeEntity ? EntityField.EntityType.Node : EntityField.EntityType.Relationship
                                ) as Field
                            }
                        );
                        allLogicalOps.Add(op);
                    });

            // process the matching patterns to update the adjacency matrix
            var entitiesPartOfNonOptionalMatch = namedMatchData.MatchPatterns
                .Where(p => p.IsOptionalMatch == false)
                .SelectMany(p => p)
                .Select(e => e.Alias)
                .Union(inheritedEntityAliases ?? Enumerable.Empty<string>())
                .Distinct();
            foreach (var matchPattern in namedMatchData.MatchPatterns)
            {
                Entity prevEnt = matchPattern.First();
                int prevEntIdx = aliasTable[prevEnt.Alias].Order;

                foreach (var ent in matchPattern.Skip(1))
                {
                    var curEntIdx = aliasTable[ent.Alias].Order;
                    // left join only if current is pattern is optional match
                    // and one of the entity already appears in non-optional match
                    // pattern or is inherited from previous query part
                    bool isLeftJoin = 
                        matchPattern.IsOptionalMatch &&
                        (entitiesPartOfNonOptionalMatch.Contains(prevEnt.Alias) != entitiesPartOfNonOptionalMatch.Contains(ent.Alias));
                    switch (entityJoinMatrix[prevEntIdx, curEntIdx])
                    {
                        case JoinOperator.JoinType.Cross:
                        case JoinOperator.JoinType.Left:
                            // if CROSS or LEFT, left it up to LEFT or INNER, respectively
                            entityJoinMatrix[prevEntIdx, curEntIdx] = isLeftJoin ? JoinOperator.JoinType.Left : JoinOperator.JoinType.Inner;
                            entityJoinMatrix[curEntIdx, prevEntIdx] = entityJoinMatrix[prevEntIdx, curEntIdx];
                            break;
                        default:
                            // do nothing if it is INNER, already highest constrained join
                            break;
                    }

                    prevEnt = ent;
                    prevEntIdx = aliasTable[ent.Alias].Order;
                }
            }

            // get the transitive closure of the DAG
            var entityJoinMatrixClosure = entityJoinMatrix.TransitiveClosure();

            // we update the logical operator table (logicOpSrcTable) based on the match patterns
            // we do it in 3 passes:
            //   - inner join first
            //   - left join second
            //   - cross join last
            // 
            // e.g. Match (a)-[b]-(c), (e), (a)-[d]-(c)
            // Start:
            //       a, DataSource(a)
            //       b, DataSource(b)
            //       c, DataSource(c)
            //       d, DataSource(d)
            //       e, DataSource(e)
            // After processing the first MatchPattern: (a)-[b]-(c):
            //       a, Join(Join(DataSource(a), DataSource(b)), DataSource(c))
            //       b, Join(Join(DataSource(a), DataSource(b)), DataSource(c))
            //       c, Join(Join(DataSource(a), DataSource(b)), DataSource(c))
            //       d, DataSource(d)
            //       e, DataSource(e)
            // After processing the second MatchPattern: (e)
            //       // no change as (e) does not have
            // After processing the third MatchPattern: (a)-[d]-(c)
            //       a, Join(Join(Join(DataSource(a), DataSource(b)), DataSource(c)), DataSource(d))
            //       b, Join(Join(Join(DataSource(a), DataSource(b)), DataSource(c)), DataSource(d))
            //       c, Join(Join(Join(DataSource(a), DataSource(b)), DataSource(c)), DataSource(d))
            //       d, Join(Join(Join(DataSource(a), DataSource(b)), DataSource(c)), DataSource(d))
            //       e, DataSource(e)
            // Post processing by cross product any disjoint parts:
            //       a, CrossJoin(Join(Join(Join(DataSource(a), DataSource(b)), DataSource(c)), DataSource(d)), DataSource(e))
            //       b, CrossJoin(Join(Join(Join(DataSource(a), DataSource(b)), DataSource(c)), DataSource(d)), DataSource(e))
            //       c, CrossJoin(Join(Join(Join(DataSource(a), DataSource(b)), DataSource(c)), DataSource(d)), DataSource(e))
            //       d, CrossJoin(Join(Join(Join(DataSource(a), DataSource(b)), DataSource(c)), DataSource(d)), DataSource(e))
            //       e, CrossJoin(Join(Join(Join(DataSource(a), DataSource(b)), DataSource(c)), DataSource(d)), DataSource(e))

            // get a list of unique field joins need to be performed from the traversals
            var joinPairs = new Dictionary<KeyValuePair<string, string>, JoinOperator.JoinKeyPair>();
            foreach (var matchPattern in namedMatchData.MatchPatterns)
            {
                for (var patIdx = 1; patIdx < matchPattern.Count; patIdx++)
                {
                    // the order is that if node is the src of edge, then [node, edge]
                    // otherwise [edge, node]
                    Entity prevEnt = matchPattern[patIdx-1];
                    var ent = matchPattern[patIdx];
                    var joinPair = GetJoinKeyPairInProperOrder(prevEnt, ent);
                    var key = new KeyValuePair<string, string>(joinPair.NodeAlias, joinPair.RelationshipOrNodeAlias);
                    JoinOperator.JoinKeyPair existingJoinPair;
                    if (joinPairs.TryGetValue(key, out existingJoinPair))
                    {
                        // update/validate if already observed the same join and maybe of different type
                        // update paths:
                        //    either -> { left, right, both }
                        //    left -> {both}
                        //    right -> {both}
                        // all others are invalid if not equal
                        if (existingJoinPair.Type != joinPair.Type)
                        {
                            if (existingJoinPair.Type == JoinOperator.JoinKeyPair.JoinKeyPairType.Either ||
                                joinPair.Type == JoinOperator.JoinKeyPair.JoinKeyPairType.Both)
                            {
                                joinPairs[key] = joinPair;
                            }
                            else
                            {
                                // conflict
                                throw new TranspilerBindingException($"Conflicting traversal detected between '{joinPair.NodeAlias}' and '{joinPair.RelationshipOrNodeAlias}'");
                            }
                        }
                    }
                    else
                    {
                        joinPairs.Add(key, joinPair);
                    }
                }
            }

            // pass 'inner join' then pass 'left join'
            for (var passType = JoinOperator.JoinType.Inner; passType >= JoinOperator.JoinType.Left; passType--)
            {
                foreach (var matchPattern in namedMatchData.MatchPatterns)
                {
                    Entity prevEnt = matchPattern.First();
                    int prevEntIdx = aliasTable[prevEnt.Alias].Order;
                    
                    for (var patIdx = 1; patIdx < matchPattern.Count; patIdx++)
                    {
                        var ent = matchPattern[patIdx];
                        var curEntIdx = aliasTable[ent.Alias].Order;
                        var joinType = entityJoinMatrixClosure[prevEntIdx, curEntIdx];
                        var joinPair = GetJoinKeyPairInProperOrder(prevEnt, ent);

                        if (hasJoined[prevEntIdx, curEntIdx] || joinType < passType)
                        {
                            // skip if already joined or passtype delays the join to later pass
                        }
                        else
                        {
                            // create a new join operator, with the left side operator
                            // be the left side of the LEFT OUTTER JOIN if it is outter join (inner join order doesn't matter)
                            var leftOp = logicOpSrcTable[prevEnt.Alias];
                            var rightOp = logicOpSrcTable[ent.Alias];
                            var newOp = new JoinOperator(
                                leftOp,
                                rightOp,
                                entityJoinMatrixClosure[prevEntIdx, curEntIdx] // join type
                                );
                            var fields = leftOp.OutputSchema.Union(rightOp.OutputSchema)
                                .Select(f => f.Clone())
                                .GroupBy(f => f.FieldAlias)
                                .Select(f => f.First());
                            newOp.InputSchema = new Schema(fields);
                            newOp.OutputSchema = new Schema(fields);

                            // calculate join needed by this operator
                            var entAliasesJointTogether = leftOp.OutputSchema.Where(s => s is EntityField).Select(s => s.FieldAlias)
                                .Union(
                                    rightOp.OutputSchema.Where(s => s is EntityField).Select(s => s.FieldAlias)
                                ).Distinct();
                            var joinKeysRequiredForThisJoin = joinPairs
                                .Where(kv => entAliasesJointTogether.Contains(kv.Key.Key) && entAliasesJointTogether.Contains(kv.Key.Value))
                                .ToList();
                            joinKeysRequiredForThisJoin.ForEach(kv =>
                            {
                                // add join to this particular operator
                                newOp.AddJoinPair(kv.Value);
                                // remove from unsatisfied join key list
                                joinPairs.Remove(kv.Key);
                            });

                            // add newly created logical operator to the all logical operator list
                            allLogicalOps.Add(newOp);

                            // update the Logical Operator lookup table
                            var aliasToUpdateOp = logicOpSrcTable.Where(kv => kv.Value == leftOp || kv.Value == rightOp).Select(kv => kv.Key).ToList();
                            aliasToUpdateOp.ForEach(a => logicOpSrcTable[a] = newOp);

                            // update the fact if two table already jointed not
                            hasJoined[prevEntIdx, curEntIdx] = true;
                            hasJoined[curEntIdx, prevEntIdx] = true;
                            hasJoined = hasJoined.TransitiveClosure(boolComparer);
                        }

                        prevEnt = ent;
                        prevEntIdx = aliasTable[ent.Alias].Order;
                    }
                }
            }

            // pass 'cross join': all the remaining segments to create the logical operators
            var joiningSegments = logicOpSrcTable.Values.Union(additionalCrossJoinOp).Distinct();
            LogicalOperator finalOp = joiningSegments.Count() > 1 ? joiningSegments.Aggregate(
                (x, delta) =>
                {
                    var newOp = new JoinOperator(x, delta, JoinOperator.JoinType.Cross);
                    var fields = x.OutputSchema.Union(delta.OutputSchema)
                            .Select(f => f.Clone())
                            .GroupBy(f => f.FieldAlias)
                            .Select(f => f.First());
                    newOp.InputSchema = new Schema(fields);
                    newOp.OutputSchema = new Schema(fields);
                    allLogicalOps.Add(newOp);
                    return newOp;
                }
            ) : joiningSegments.First();

            // for any relationships of same type appearing in the same match statement, adding
            // a condition that it should not be repeated. E.g., for
            //   MATCH (p:Person)-[a1:Acted_In]->(m:Movie)<-[a2:Acted_In]-(p2:Person) ...
            // we will add a condition that
            //   a1.SrcId <> a2.SrcId AND a1.SinkId <> a2.SinkId
            var relationshipTypeGroups = namedMatchData
                .AllEntitiesOrdered
                .Where(e => e is RelationshipEntity)
                .Cast<RelationshipEntity>()
                .SelectMany(e =>
                {
                    switch (e.RelationshipDirection)
                    {
                        case RelationshipEntity.Direction.Forward:
                            return new(string Alias, string EntityFullName)[1] { (e.Alias, $"{e.LeftEntityName}@{e.EntityName}@{e.RightEntityName}") };
                        case RelationshipEntity.Direction.Backward:
                            return new(string Alias, string EntityFullName)[1] { (e.Alias, $"{e.RightEntityName}@{e.EntityName}@{e.LeftEntityName}") };
                        default:
                            Debug.Assert(e.RelationshipDirection == RelationshipEntity.Direction.Both);
                            return new(string Alias, string EntityFullName)[2]
                            {
                                (e.Alias, $"{e.LeftEntityName}@{e.EntityName}@{e.RightEntityName}"),
                                (e.Alias, $"{e.RightEntityName}@{e.EntityName}@{e.LeftEntityName}"),
                            };
                    }
                })
                .GroupBy(e => e.EntityFullName)
                .Where(e => e.Count() > 1)
                .ToList();

            if (relationshipTypeGroups.Count > 0)
            {
                var unexpandedInequalityConditions = relationshipTypeGroups
                    .Aggregate(
                        new List<(string RelAlias1, string RelAlias2)>(),
                        (list, g) => {
                            var cond = g.ToList();
                            for (int i = 0; i < cond.Count-1; i++)
                            {
                                for (int j = i + 1; j < cond.Count; j++)
                                {
                                    list.Add((cond[i].Alias, cond[j].Alias));
                                }
                            }
                            return list;
                        }
                    );
                finalOp = CreateLogicalTree(unexpandedInequalityConditions, finalOp, allLogicalOps);
            }

            return finalOp;
        }

        /// <summary>
        /// Top down propagation of the data types of each fields in each operators' output schema
        /// </summary>
        private void PropagateDataTypes()
        {
            var allOperators = StartingOperators
                .SelectMany(op => op.GetAllDownstreamOperatorsOfType<LogicalOperator>())
                .Distinct()
                .GroupBy(op => op.Depth)
                .OrderBy(g => g.Key);  // top-down direction for type propagation

            Debug.Assert(allOperators.First().All(op => op is StartLogicalOperator));

            // Note: first level are start operators and have schema populated during binding
            foreach (var opGrp in allOperators.Skip(1))
            {
                opGrp.ToList().ForEach(op => op.PropagateDateTypesForSchema());
            }
        }

        /// <summary>
        /// Update the list of actual fields referenced for each entity alias in the return 
        /// body or clauses such as WHERE, ORDER BY, etc.
        /// This helps to trim out unreferenced fields early in the logical plan tree
        /// </summary>
        private void UpdateActualFieldReferencesForEntityFields()
        {
            var allOperators = StartingOperators
                .SelectMany(op => op.GetAllDownstreamOperatorsOfType<LogicalOperator>())
                .Distinct()
                .GroupBy(op => op.Depth)
                .OrderByDescending(g => g.Key); // Bottom-up to keep accumulating until the source
            foreach (var opGrp in allOperators)
            {
                opGrp.ToList().ForEach(op => op.PropagateReferencedPropertiesForEntityFields());
            }
        }
    }
}
