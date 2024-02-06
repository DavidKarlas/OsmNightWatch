
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers.BrokenCoastline
{
    internal class CoastlineValidationTest
    {
        class Graph
        {
            public Dictionary<long, (long wayId, long firstNode, long lastNode)> FirstNodes = new();
            public Dictionary<long, (long wayId, long firstNode, long lastNode)> LastNodes = new();
            public Dictionary<long, List<(long wayId, long firstNode, long lastNode)>> DuplicateFirstNodes = new();
            public Dictionary<long, List<(long wayId, long firstNode, long lastNode)>> DuplicateLastNodes = new();
        }

        public (List<(long nodeId, string issueDetails)>, List<(long wayId, string issueDetails)>) Visit(IEnumerable<(uint id, long firstNode, long lastNode)> coastline)
        {
            Graph graph = new();
            List<(long nodeId, string issueDetails)> issueNodes = new();
            List<(long wayId, string issueDetails)> issueWays = new();

            // Create graph for coastline
            foreach (var (wayId, first, last) in coastline)
            {
                // Check if start or end nodes already exist
                bool foundDuplicateFirst = false;
                bool foundDuplicateLast = false;

                if (graph.FirstNodes.ContainsKey(first))
                {
                    var ways = new List<(long wayId, long firstNode, long lastNode)>() { (wayId, first, last), graph.FirstNodes[first] };
                    graph.DuplicateLastNodes[first] = ways;
                    foundDuplicateFirst = true;
                }

                if (graph.LastNodes.ContainsKey(last))
                {
                    var ways = new List<(long wayId, long firstNode, long lastNode)>() { (wayId, first, last), graph.LastNodes[last] };
                    graph.DuplicateLastNodes[last] = ways;
                    foundDuplicateLast = true;
                }

                // Remove start or ends if they already have a pair
                if (graph.FirstNodes.ContainsKey(last))
                {
                    graph.FirstNodes.Remove(last);
                }
                else if (!foundDuplicateLast)
                {
                    graph.LastNodes.Add(last, (wayId, first, last));
                }

                if (graph.LastNodes.ContainsKey(first))
                {
                    graph.LastNodes.Remove(first);
                }
                else if (!foundDuplicateFirst)
                {
                    graph.FirstNodes.Add(first, (wayId, first, last));
                }
            }

            // Find ways with wrong direction
            issueWays = issueWays.Concat(FindWaysWrongDirection(graph)).ToList();

            // Find unconnected nodes in the coastline
            issueNodes = FindUnconnectedNodes(graph);

            return (issueNodes, issueWays);
        }

        List<(long wayId, string issueDetails)> FindWaysWrongDirection(Graph graph)
        {
            List<(long wayId, string issueDetails)> found = new();
            if (graph.DuplicateFirstNodes.Count > 0)
            {
                // TODO: Add part to check if there are two connected ways with wrong direction
                foreach (var nodeFirst in graph.DuplicateFirstNodes.Keys)
                {
                    foreach (var nodeLast in graph.DuplicateLastNodes.Keys)
                    {
                        var listOfWayIds = graph.DuplicateFirstNodes[nodeFirst].Select(p => p.wayId).Intersect(graph.DuplicateLastNodes[nodeLast].Select(p => p.wayId)).ToList();
                        if (listOfWayIds.Count > 0)
                        {
                            var wayId = listOfWayIds.First();
                            found.Add((wayId, "Way with wrong direction"));
                            graph.FirstNodes.Remove(nodeFirst);
                            graph.LastNodes.Remove(nodeLast);
                        }
                    }
                }
            }
            return found;
        }

        List<(long nodeId, string issueDetails)> FindUnconnectedNodes(Graph graph)
        {
            List<(long nodeId, string issueDetails)> unconnectedNodes = new();
            if (graph.FirstNodes.Count > 0)
            {
                foreach (var node in graph.FirstNodes)
                {
                    unconnectedNodes.Add((node.Key, "Unconnected node in the coastline"));
                }
            }

            if (graph.LastNodes.Count > 0)
            {
                foreach (var node in graph.LastNodes)
                {
                    unconnectedNodes.Add((node.Key, "Unconnected node in the coastline"));
                }
            }

            return unconnectedNodes;
        }
    }
}
