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
    public class LogicalPlan
    {
        private ILoggable _logger;

        public LogicalPlan(ILoggable logger)
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
        public static LogicalPlan ProcessQueryTree(QueryTreeNode treeRoot, IGraphSchemaProvider graphDef, ILoggable logger = null)
        {
            var logicalPlanner = new LogicalPlan(logger);
            var allLogicalOps = new HashSet<LogicalOperator>();

            // from AST, create the logical plan
            var logicalRoot = logicalPlanner.CreateLogicalTree(treeRoot, allLogicalOps);
            logicalPlanner.StartingOperators = logicalRoot.GetAllUpstreamOperatorsOfType<StartLogicalOperator>().ToList();
            logicalPlanner.TerminalOperators = new List<LogicalOperator>(1) { logicalRoot };

            // bind logical tree with the graph schema
            var bindableStartingOp = logicalPlanner.StartingOperators.Where(op => op is IBindable);
            bindableStartingOp.ToList().ForEach(op => (op as IBindable).Bind(graphDef));

            // propagate down the schema from starting operator and data types (top down)
            logicalPlanner.PropagateDataTypes();

            // expand and optimize out columns not used (bottom up)
            logicalPlanner.UpdateActualFieldReferencesForEntityFields();

            // note: in this function, we only deal with graph definition.
            //       later in code generatation phase, we deal with Cosmos storage descriptors
            //       which then worries the details of what streams to pull

            // verify that no dangling operator exists
            var i = 0;
            foreach (var op in allLogicalOps)
            {
                op.OperatorDebugId = ++i;
            }

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
        private LogicalOperator CreateLogicalTree(QueryTreeNode treeNode, ISet<LogicalOperator> allLogicalOps)
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


            // inorder walk of the tree to evaulate individual queries
            if (treeNode is SingleQueryTreeNode)
            {
                // single query
                return CreateLogicalTree(treeNode as SingleQueryTreeNode, allLogicalOps);
            }
            else
            {
                Debug.Assert(treeNode is InfixQueryTreeNode);
                return CreateLogicalTree(treeNode as InfixQueryTreeNode, allLogicalOps);
            }
        }

        /// <summary>
        /// Create logical oeprator tree for sub-tree node type InfixQueryTreeNode
        /// </summary>
        /// <param name="treeNode"></param>
        /// <returns></returns>
        private BinaryLogicalOperator CreateLogicalTree(InfixQueryTreeNode treeNode, ISet<LogicalOperator> allLogicalOps)
        {
            var left = CreateLogicalTree(treeNode.LeftQueryTreeNode, allLogicalOps);
            var right = CreateLogicalTree(treeNode.RightQueryTreeNode, allLogicalOps);

            // TODO: better way of ensure schema is the same
            Debug.Assert(left.OutputSchema.Count == left.OutputSchema.Count);

            SetOperator.SetOperationType op;
            switch (treeNode.Operator)
            {
                case InfixQueryTreeNode.QueryOperator.Union:
                    op = SetOperator.SetOperationType.Union;
                    break;
                case InfixQueryTreeNode.QueryOperator.UnionAll:
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
        /// Create logical operator for AST node type SingleQueryTreeNode
        /// </summary>
        /// <param name="treeNode"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(SingleQueryTreeNode treeNode, ISet<LogicalOperator> allLogicalOps)
        {
            var stack = new Stack<PartialQueryTreeNode>();
            PartialQueryTreeNode lastVisited = treeNode.PipedData;
            
            while (lastVisited != null)
            {
                stack.Push(lastVisited);
                lastVisited = lastVisited.PipedData;
            }

            // maintain a list of query parts that we may collapse for optimization
            var pureMatchChainQueries = new List<PartialQueryTreeNode>();
            // maintain a pointer to the last logical operator that need to chained into next operator to be created
            LogicalOperator lastOperator = null;
            PartialQueryTreeNode curNode = null;
            PartialQueryTreeNode prevNode = null;
            var finalProjectedExprs = treeNode.EntityPropertySelected;
            while (stack.Any())
            {
                prevNode = curNode;
                curNode = stack.Pop();
                lastOperator = CreateLogicalTree(curNode, lastOperator, allLogicalOps, prevNode);
            }

            // chain with another selection operator there are ORDER BY + LIMIT combination clause 
            // Single ORDER BY or Single LIMIT will not be compiled in SCOPE, skipped here
            if (treeNode.EntityRowsLimit?.Count() > 0 && treeNode.EntityPropertyOrderBy?.Count() > 0)
            {
                var limitExpr = treeNode.EntityRowsLimit;
                var orderExpr = treeNode.EntityPropertyOrderBy;
                lastOperator = CreateLogicalTree(orderExpr, limitExpr, lastOperator, allLogicalOps);

                // TODO: Add more expression that came from previous partial query tree node
                var appendedProjExpr = treeNode.EntityPropertySelected;
                var newProjExpr = curNode?.ProjectedExpressions?.Where(n => appendedProjExpr.All(p => p.Alias != n.Alias));

                // flag true if any selected properties has aggregation function.
                var hasAggregationExpression = false;
                if (treeNode.EntityPropertySelected.Any(n => n.GetChildrenQueryExpressionType<QueryExpressionAggregationFunction>().Count() > 0))
                {
                    hasAggregationExpression = true;
                }

                appendedProjExpr = (newProjExpr == null || treeNode.IsDistinct || hasAggregationExpression? appendedProjExpr : appendedProjExpr.Union(newProjExpr).ToList());
                lastOperator = CreateLogicalTree(appendedProjExpr, treeNode.IsDistinct, lastOperator, allLogicalOps);

                finalProjectedExprs = finalProjectedExprs.Select(n =>
                {
                    var property = n.GetChildrenQueryExpressionType<QueryExpressionProperty>().FirstOrDefault();
                    if (property?.Entity == null)
                    {
                        return new QueryExpressionWithAlias
                        {
                            Alias = n.Alias,
                            InnerExpression = new QueryExpressionProperty()
                            {
                                VariableName = n.Alias,
                                DataType = null,
                                Entity = null,
                                PropertyName = null
                            }
                        };
                    }
                    else
                    {
                        return n;
                    }
                }).ToList();

                // if distinct case, we currently don't support entity field in the order by statement.
                // For example:
                //   RETURN DISTINCT a.Name as Name, b.Address as Address
                //   ORDER BY a.Name
                //   LIMIT 10
                if ((treeNode.IsDistinct || hasAggregationExpression) && orderExpr.Any(n =>
                 {
                     var properties = n.GetChildrenQueryExpressionType<QueryExpressionProperty>();
                     if(properties != null && properties.Any(k => k.PropertyName!=null && k.VariableName != null))
                     {
                         return true;
                     }
                     return false;
                 }))
                {
                    throw new TranspilerNotSupportedException("ORDER BY X.XXX under RETURN DISTINCT clause/RETURN with aggregation function");
                }
                
            }

            // add the final projection operator for the fields select in the wrapping Single Query tree node, which 
            // has only Projection and PipedData
            Debug.Assert(lastOperator != null);
            Debug.Assert((treeNode.EntityPropertySelected?.Count ?? 0) > 0);

            // Block the attempt to return entities as we don't support this anywhere today
            if (treeNode.EntityPropertySelected.Any(p => p.TryGetDirectlyExposedEntity() != null))
            {
                throw new TranspilerNotSupportedException
                    ($"Query final return body returns the whole entity {treeNode.EntityPropertySelected.First(p => p.TryGetDirectlyExposedEntity() != null).Alias} instead of its fields");
            }

            var finalProjOperator = CreateLogicalTree(finalProjectedExprs, treeNode.IsDistinct, lastOperator, allLogicalOps);
            return finalProjOperator;
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
        /// creating selection operator for ORDER BY ... LIMIT  clause
        /// </summary>
        /// <param name="exp"></param>
        /// <param name="pipedData"></param>
        /// <param name="allLogicalOps"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(
            IList<QueryExpressionOrderBy> orderExp, 
            IList<QueryExpressionLimit> limitExp,
            LogicalOperator pipedData, 
            ISet<LogicalOperator> allLogicalOps)
        {
            var selectionOp = new SelectionOperator(pipedData, orderExp, limitExp);
            var selectionSchema = new Schema(pipedData.OutputSchema);

            // append new property from ORDER BY clause to the the input and output schema
            
            selectionOp.InputSchema = new Schema(selectionSchema);
            selectionOp.OutputSchema = new Schema(selectionSchema); 
            allLogicalOps.Add(selectionOp);
            return selectionOp;
        }
        
        /// <summary>
        /// Create filtering operator for unexpanded inequality expression
        /// </summary>
        /// <param name="exp"></param>
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
                    return new SingleField(
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
        /// Create join operator to join two set of operators with a list of entity aliases of instances of node which their id should be used in equi-join
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="entityAliasesToJoin">the list of aliases (in the output of left/right) that represents entity instances and should be used in nodeid equi-join</param>
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
        /// Create logical operators for a partial query part
        /// </summary>
        /// <param name="treeNode"></param>
        /// <param name="pipedData"></param>
        /// <param name="allLogicalOps"></param>
        /// <returns></returns>
        private LogicalOperator CreateLogicalTree(PartialQueryTreeNode treeNode, LogicalOperator pipedData, ISet<LogicalOperator> allLogicalOps, PartialQueryTreeNode previousNode)
        {
            LogicalOperator lastOperator = pipedData;

            if (treeNode.MatchData != null)
            {
                // comes with additional Match statements
                var isOptionalMatch = treeNode.MatchData.MatchPatterns.Any(p => p.IsOptionalMatch);
                Debug.Assert(treeNode.MatchData.MatchPatterns.All(p => p.IsOptionalMatch == isOptionalMatch));

                if (treeNode.PostCondition != null)
                {
                    // with filtering conditions, optional match requires a fork to apply filtering first and then rejoin with piped data, 
                    // where non-optional doesn't require it
                    // e.g. MATCH ... OPTIONAL MATCH ... WHERE ... WITH/RETURN ...
                    if (isOptionalMatch)
                    {
                        // apply filtering conditions to the optional match pattern then left outter join back with the piped data
                        var modifiedMatch = new MatchDataSource()
                        {
                            MatchPatterns = treeNode.MatchData.MatchPatterns.Select(p => new MatchPattern(false, p)).ToList()
                        };
                        var omOperator = CreateLogicalTree(modifiedMatch, lastOperator, allLogicalOps);
                        var condOmOperator = CreateLogicalTree(treeNode.PostCondition, omOperator, allLogicalOps);

                        var leftEntityAliases = lastOperator.OutputSchema
                            .Where(f => f is EntityField).Select(f => (f as EntityField).FieldAlias);
                        var explicitlyMatchedAliasesToJoinWithLeft = modifiedMatch.AllEntitiesOrdered
                            .Select(e => e.Alias)
                            .Where(a => a != null)
                            .Intersect(leftEntityAliases);
                        lastOperator = CreateLogicalTree(lastOperator, condOmOperator, explicitlyMatchedAliasesToJoinWithLeft, true, allLogicalOps);
                    }
                    else
                    {
                        // join with match data
                        lastOperator = CreateLogicalTree(treeNode.MatchData, lastOperator, allLogicalOps);
                        // apply filtering
                        lastOperator = CreateLogicalTree(treeNode.PostCondition, lastOperator, allLogicalOps);
                    }
                }
                else
                {
                    // without filtering conditions, optional and non-optional match are processed similarly (except for the join type)
                    // e.g. MATCH ... OPTIONAL MATCH ... WITH/RETURN ...
                    lastOperator = CreateLogicalTree(treeNode.MatchData, lastOperator, allLogicalOps);
                }
                // Do projection
                lastOperator = CreateLogicalTree(treeNode.ProjectedExpressions, treeNode.IsDistinct, lastOperator, allLogicalOps);
            }
            else
            {
                // Order By need to pair with limit in order to compilable in scope
                var isValidOrderByClausePair = (treeNode.LimitExpression?.Count() > 0 && treeNode.OrderByExpression?.Count() > 0);
                var isExtraCollectionProjectionNeeded = (treeNode.PostCondition != null || isValidOrderByClausePair);
                var appendedProjExpr = treeNode.ProjectedExpressions;
                var hasAggregationExpression = false;

                if (treeNode.ProjectedExpressions.SelectMany(n => n.GetChildrenQueryExpressionType<QueryExpressionAggregationFunction>()).Count() > 0)
                {
                    hasAggregationExpression = true;
                }

                //  "DISTINCT" acted like another "WITH" clause: downstream ORDER BY, WHERE can only use field from WITH clause
                //      if no DISTINCT, collect field information from previous tree node
                //      if has DISTINCT, no need to collect from previous one for the behavior of DISTINCT descrived above
                //  Example: 
                //  WITH DISTINCT a.Name as Name, b.Title as Title
                //  ORDER BY Name, Title
                //  WHERE Name <> "Tom"
                //  RETURN Name, Title
                //  ** ORDER BY/WHERE can only refer Name, Title as there is a DISTINCT
                //
                //  if no distinct 
                //  WITH a.Name as Name, b.Title as Title
                //  ORDER BY a.Tagline, n.Price
                //  WHERE Name <> "Tom"
                //  RETURN Name, Title
                //  ** ORDER BY/WHERE can refer any field in a and b


                if (!treeNode.IsDistinct && previousNode != null && !hasAggregationExpression)
                {
                    var newProjExpr = previousNode.ProjectedExpressions.Where(n => appendedProjExpr.All(p =>p.Alias!= n.Alias));
                    appendedProjExpr = (newProjExpr== null? appendedProjExpr: appendedProjExpr.Union(newProjExpr).ToList());
                    lastOperator = CreateLogicalTree(appendedProjExpr, treeNode.IsDistinct, lastOperator, allLogicalOps);
                }
                else
                {
                    lastOperator = CreateLogicalTree(treeNode.ProjectedExpressions, treeNode.IsDistinct, lastOperator, allLogicalOps);
                }

                // Do trimming the non-entity field by removing redundant calculation and changed it to Alias AS Alias
                // The reason for trimming: related calculation was already done in previous steps. Only need to refer the alias and return.
                var trimmedProjExpr = appendedProjExpr.Select(n =>
                {
                    var property = n.GetChildrenQueryExpressionType<QueryExpressionProperty>().FirstOrDefault();
                    if (property?.Entity == null)
                    {
                        return new QueryExpressionWithAlias
                        {
                            Alias = n.Alias,
                            InnerExpression = new QueryExpressionProperty()
                            {
                                VariableName = n.Alias,
                                DataType = null,
                                Entity = null,
                                PropertyName = null
                            }
                        };
                    }
                    else
                    {
                        return n;
                    }
                }).ToList();

                // chain with another selection operator there are ORDER BY + LIMIT combination clause 
                // Single ORDER BY or Single LIMIT will not be compiled in SCOPE, skipped here
                if (isValidOrderByClausePair)
                {
                    var limitExpr = treeNode.LimitExpression;
                    var orderExpr = treeNode.OrderByExpression;
                    lastOperator = CreateLogicalTree(orderExpr, limitExpr, lastOperator, allLogicalOps);                 
                    // projection operator "after order by" selection operator
                    lastOperator = CreateLogicalTree(trimmedProjExpr, treeNode.IsDistinct, lastOperator, allLogicalOps);
                }

                if (treeNode.PostCondition != null)
                {
                    // without match, we can just apply filtering if needed (e.g. WITH ... WHERE ...)
                    lastOperator = CreateLogicalTree(treeNode.PostCondition, lastOperator, allLogicalOps);
                    // projection operator after "where" selection operator
                    lastOperator = CreateLogicalTree(trimmedProjExpr, treeNode.IsDistinct, lastOperator, allLogicalOps);
                }
                // Do the last projection in the WITH statement.
                // Since all the schema was already mapped to the target aliases in the previous projection operators, only direct mapping needed.
                // target -> target
                //      eg. a.Name as Name : Name -> Name
                //          a as b: b -> b (at here 'a' can be a entity or can be a property.
                var finalProjExpr = treeNode.ProjectedExpressions.Select(n => 
                {
                    var property = n.GetChildrenQueryExpressionType<QueryExpressionProperty>().FirstOrDefault();
                    return new QueryExpressionWithAlias
                    {
                        Alias = n.Alias,
                        InnerExpression = new QueryExpressionProperty()
                        {
                            VariableName = n.Alias,
                            DataType = null,
                            Entity = property?.Entity,
                            PropertyName = null
                        }
                    };
                }).ToList();

            lastOperator = CreateLogicalTree(finalProjExpr, treeNode.IsDistinct, lastOperator, allLogicalOps);
            }
            return lastOperator;
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
                    // TODO: in future, we can support this and set the joinKeyType to .Both. This requires
                    //       we add support in codegen to produce a data source that has src/sink key reversed unioned with itself
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
        private LogicalOperator CreateLogicalTree(MatchDataSource matchData, LogicalOperator pipedData, ISet<LogicalOperator> allLogicalOps)
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

            // build an adjancency matrix represent the DAG for the join logical operator each alias in the form of an array[Dim1, Dim2]
            // note that the list of DataSourceOperator are added to allLogicalOps later in the code after recociling with inherited entities
            var logicOpSrcTable = aliasTable.ToDictionary(kv => kv.Key, kv => new DataSourceOperator(kv.Value.Entity) as LogicalOperator);

            // build adjancency matrix with element being the join type between the two entities
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

            // process the matching patterns to update the adjancency matrix
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
                    // pattern or is inhertied from previous query part
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
            // After processin the second MatchPattern: (e)
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
                        // update/validate if already observed the same join and maybe of differen type
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

                            // caculate join needed by this operator
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
                                // remove from unsatisified join key list
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
        /// Evaluate field data types for each operators' output schema (after data schema is bound)
        /// </summary>
        private void PropagateDataTypes()
        {
            // we go top down for each layers in the logical plan to propagate field's data types

            var allOperators = StartingOperators
                .SelectMany(op => op.GetAllDownstreamOperatorsOfType<LogicalOperator>())
                .Distinct()
                .GroupBy(op => op.Depth)
                .OrderBy(g => g.Key);

            Debug.Assert(allOperators.First().All(op => op is StartLogicalOperator));

            // We assume that first level is taken care off in previous stage (data binding)
            // We start from second level and down to propagate data types
            foreach (var opGrp in allOperators.Skip(1))
            {
                opGrp.ToList().ForEach(op => op.PropagateSchema());
            }
        }

        /// <summary>
        /// Update the list of actual properties that got referenced for each entity field
        /// </summary>
        private void UpdateActualFieldReferencesForEntityFields()
        {
            // we go bottom up to propagate entity fields referenced in return body or condition clauses
            // that each operator's input/ouput schema must carry

            var allOperators = StartingOperators
                .SelectMany(op => op.GetAllDownstreamOperatorsOfType<LogicalOperator>())
                .Distinct()
                .GroupBy(op => op.Depth)
                .OrderByDescending(g => g.Key);
            foreach (var opGrp in allOperators)
            {
                opGrp.ToList().ForEach(op => op.PropagateReferencedPropertiesForEntityFields());
            }
        }
    }
}
