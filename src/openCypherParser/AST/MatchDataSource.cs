/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

using System.Collections.Generic;
using System.Linq;
using openCypherTranspiler.Common.Exceptions;

namespace openCypherTranspiler.openCypherParser.AST
{
    /// <summary>
    /// represents the parsed group of matched patterns
    /// </summary>
    public class MatchDataSource : TreeNode
    {
        #region Implements TreeNode
        protected override IEnumerable<TreeNode> Children
        {
            get
            {
                return Enumerable.Empty<TreeNode>();
            }
        }
        #endregion Implements TreeNode

        /// <summary>
        /// Return all the entities refered by the Match statement
        /// with the same order it appears
        /// </summary>
        public IList<Entity> AllEntitiesOrdered
        {
            get
            {
                // can't use SelectMany if it doesn't guarantee order
                // MatchPatterns?.SelectMany(p => p) ?? Enumerable.Empty<Entity>();
                return MatchPatterns?.Aggregate(new List<Entity>(), (p, l) => { p.AddRange(l); return p; }) ?? new List<Entity>(0);
            }
        }

        /// <summary>
        /// A list of match patterns:
        ///     e.g. (a:device)-[]->(
        /// </summary>
        public IList<MatchPattern> MatchPatterns { get; set; }

        /// <summary>
        /// Return a new MatchDataSource mirrors the old one but with anonymous entities filled with
        /// a place holder alias
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public MatchDataSource AssignGeneratedEntityAliasToAnonEntities(string anonVarPrefix)
        {
            var varIdx = 0;
            var existingAliases = new HashSet<string>(MatchPatterns.SelectMany(p => p.Select(e => e.Alias)).Where(a => !string.IsNullOrEmpty(a)).Distinct());

            if (existingAliases.Any(p => p.StartsWith(anonVarPrefix)))
            {
                throw new TranspilerSyntaxErrorException($"Banned prefix for variable aliases: {anonVarPrefix}. Please consider a different prefix.");
            }

            // creating a new DS with implied labels fixed up
            var matchDsNew = new MatchDataSource()
            {
                MatchPatterns = MatchPatterns.Select(p => new MatchPattern(p.IsOptionalMatch, p.Select(e =>
                {
                    var alias = e.Alias ?? $"{anonVarPrefix}{varIdx++}";
                    var entity = e.Clone();
                    entity.Alias = alias;
                    return entity;
                }))).ToList()
            };

            return matchDsNew;
        }


        public override string ToString()
        {
            return $"Matches: Count={MatchPatterns?.Count ?? 0}, Patterns={string.Join(" ", MatchPatterns)}";
        }
    }

    /// <summary>
    /// A single match pattern is a traversal of entities (node and relationships)
    /// </summary>
    public class MatchPattern : List<Entity>, IList<Entity>
    {
        public MatchPattern(bool isOptional, IEnumerable<Entity> list) : base(list)
        {
            IsOptionalMatch = isOptional;
        }

        public bool IsOptionalMatch { get; set; }

        public override string ToString()
        {
            return $"MatchPat: IsOptional={IsOptionalMatch}; Pattern={string.Join(",", this)}";
        }
    }

    /// <summary>
    /// Represents an entity of either node or edge
    /// </summary>
    public abstract class Entity
    {
        /// <summary>
        /// Name of the entity, for edge this is just verb of the edge
        /// </summary>
        public string EntityName { get; set; }

        /// <summary>
        /// Alias used for refering the entity
        /// </summary>
        public string Alias { get; set; }

        /// <summary>
        /// Make a deep copy of the derived class of this type
        /// </summary>
        /// <returns></returns>
        public abstract Entity Clone();
    }

    public class NodeEntity : Entity
    {
        public override Entity Clone()
        {
            return new NodeEntity()
            {
                Alias = this.Alias,
                EntityName = this.EntityName
            };
        }

        public override string ToString()
        {
            return $"({Alias}:{EntityName})";
        }
    }

    /// <summary>
    /// Edge is not unique by its verb during binding but depends on the node it connects
    /// E.g. (:device)-[:runs]-(:app)
    /// so we capture the in node (left) and out node of this edge (right)
    /// </summary>
    public class RelationshipEntity : Entity
    {
        public enum Direction
        {
            Both,  // -[]-
            Forward, // -[]->
            Backward // <-[]-
        }

        
        /// <summary>
        /// Left side node entity's name
        /// Note that 'left' here the lexical order and not refelcting the direction
        /// </summary>
        public string LeftEntityName { get; set; }

        /// <summary>
        /// Right side node entity's name
        /// Note that 'right' here the lexical order and not refelcting the direction
        /// </summary>
        public string RightEntityName { get; set; }

        public override Entity Clone()
        {
            return new RelationshipEntity()
            {
                Alias = this.Alias,
                EntityName = this.EntityName,
                RelationshipDirection = this.RelationshipDirection,
                LeftEntityName = this.LeftEntityName,
                RightEntityName = this.RightEntityName,
            };
        }
        /// <summary>
        /// Name of the entity, for edge this is just verb of the edge
        /// </summary>
        public Direction RelationshipDirection { get; set; }

        public override string ToString()
        {
            return $"{LeftEntityName}{(RelationshipDirection == Direction.Backward ? "<" : "")}-[{Alias}:{EntityName}]-{(RelationshipDirection == Direction.Forward ? ">" : "")}{RightEntityName}";
        }
    }
}
