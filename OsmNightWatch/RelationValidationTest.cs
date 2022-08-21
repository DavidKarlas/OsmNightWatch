using OsmSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    internal class RelationValidationTest
    {
        class Graph
        {
            public HashSet<long> AllNodes = new();
            public HashSet<Way> Ways = new();

            public Graph(Way way)
            {
                AllNodes.UnionWith(way.Nodes);
                Ways.Add(way);
            }

            public bool FastMerge(Way way)
            {
                var nodes = way.Nodes;
                long first = nodes[0];
                long last = nodes[nodes.Length - 1];

                if (AllNodes.Contains(first) || AllNodes.Contains(last))
                {
                    AllNodes.UnionWith(way.Nodes);
                    Ways.Add(way);
                    return true;
                }
                return false;
            }

            public bool FastMerge(Graph graph)
            {
                foreach (Way way in graph.Ways)
                {
                    var nodes = way.Nodes;
                    if (AllNodes.Contains(nodes[0]) || AllNodes.Contains(nodes[nodes.Length - 1]))
                    {
                        AllNodes.UnionWith(graph.AllNodes);
                        Ways.UnionWith(graph.Ways);
                        return true;
                    }
                }
                return false;
            }

            public bool Merge(Graph graph)
            {
                foreach (long node in graph.AllNodes)
                {
                    if (AllNodes.Contains(node))
                    {
                        AllNodes.UnionWith(graph.AllNodes);
                        Ways.UnionWith(graph.Ways);
                        return true;
                    }
                }
                return false;
            }
        }

        public bool Visit(Relation r, Dictionary<long, Way> ways)
        {
            List<Graph> graphs = new();
            foreach (var member in r.Members)
            {
                if (member.Role != "inner" && member.Role != "outer")
                {
                    continue;
                }

                if (member.Type == OsmGeoType.Way)
                {
                    var way = ways[member.Id];
                    var nodes = way.Nodes;
                    if (nodes.Length < 2)
                    {
                        continue;
                    }

                    var merged = false;
                    foreach (var graph in graphs)
                    {
                        if (graph.FastMerge(way))
                        {
                            merged = true;
                            break;
                        }
                    }
                    if (merged)
                        continue;

                    graphs.Add(new Graph(way));
                }
            }
            bool mergedSomething = false;
            do
            {
                mergedSomething = false;
                foreach (Graph graph in graphs.ToArray())
                {
                    if (!graphs.Contains(graph))
                        continue;
                    foreach (Graph graph2 in graphs.ToArray())
                    {
                        if (graph == graph2)
                            continue;
                        if (!graphs.Contains(graph2))
                            continue;

                        if (graph.FastMerge(graph2))
                        {
                            mergedSomething = true;
                            graphs.Remove(graph2);
                        }
                    }
                }
            } while (mergedSomething);

            do
            {
                mergedSomething = false;
                foreach (Graph graph in graphs.ToArray())
                {
                    if (!graphs.Contains(graph))
                        continue;
                    foreach (Graph graph2 in graphs.ToArray())
                    {
                        if (graph == graph2)
                            continue;
                        if (!graphs.Contains(graph2))
                            continue;

                        if (graph.Merge(graph2))
                        {
                            mergedSomething = true;
                            graphs.Remove(graph2);
                        }
                    }
                }
            } while (mergedSomething);

            if (graphs.Count > 1)
            {
                var allLeafs = new HashSet<long>();
                int graphsWithAtLeastOneLeaft = 0;
                foreach (Graph graph in graphs)
                {
                    var leafs = CacluateLeafs(graph);
                    if (leafs.Count > 0)
                    {
                        graphsWithAtLeastOneLeaft++;
                        allLeafs.UnionWith(leafs);
                    }
                }
                if (graphsWithAtLeastOneLeaft > 1)
                {
                    return true;
                }
            }
            return false;
        }

        HashSet<long> CacluateLeafs(Graph graph)
        {
            List<long> allNodesAtEndOfWay = new();
            HashSet<long> allNodesInsideWay = new();
            foreach (Way way in graph.Ways)
            {
                var nodes = way.Nodes;
                if (nodes.Length < 2)
                {
                    continue;
                }
                allNodesAtEndOfWay.Add(nodes[0]);
                allNodesAtEndOfWay.Add(nodes[nodes.Length - 1]);
                for (int i = 1; i < nodes.Length - 1; i++)
                {
                    allNodesInsideWay.Add(nodes[i]);
                }
            }
            var distinctEndingNodes = new HashSet<long>();
            var moreThan1EndingNode = new HashSet<long>();
            foreach (var node in allNodesAtEndOfWay)
            {
                if (!distinctEndingNodes.Add(node))
                {
                    // OK, this node is 2x in allNodesAtEndOfWay
                    // we consider this as connected node...
                    moreThan1EndingNode.Add(node);
                }
            }
            var lonelyNodesAtEnd = new HashSet<long>();
            foreach (var node in distinctEndingNodes)
            {
                if (!moreThan1EndingNode.Contains(node))
                {
                    lonelyNodesAtEnd.Add(node);

                }
            }
            var trulyLonelyNodesAtEnd = new HashSet<long>();
            foreach (var node in lonelyNodesAtEnd)
            {
                // Filter out nodes that are inside scanned ways
                if (!allNodesInsideWay.Contains(node))
                    trulyLonelyNodesAtEnd.Add(node);
            }
            return trulyLonelyNodesAtEnd;
        }
    }
}
