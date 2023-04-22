
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers.BrokenCoastline
{
    internal class CoastlineValidationTest
    {
        class Graph
        {
            public Dictionary<long, Way> FirstNodes = new();
            public Dictionary<long, Way> LastNodes = new();
            public Dictionary<long, List<Way>> DuplicateFirstNodes = new(); 
            public Dictionary<long, List<Way>> DuplicateLastNodes = new();
        }

        public (List<(long nodeId, int issueDetails)>,List<(Way way, int issueDetails)>) Visit(IEnumerable<OsmGeo> coastline, IOsmGeoSource db)
        {
            Graph graph = new();
            List<(long nodeId, int detailsNum)> issueNodes = new();
            List<(Way way, int detailsNum)> issueWays = new();

            // Create graph for coastline
            foreach (var line in coastline)
            {
                if (line is Way way)
                {
                    var nodes = way.Nodes;
                    long first = nodes[0];
                    long last = nodes[nodes.Length - 1];
                    
                    if (first == last)
                    {
                        // Check if way doesn't form a proper ring
                        if (nodes.Length < 4) 
                        {
                            issueWays.Add((way, 3));
                        }

                    }
                    else
                    {
                        // Check if start or end nodes already exist
                        bool foundDuplicateFirst = false;
                        bool foundDuplicateLast = false;
                        
                        if (graph.FirstNodes.ContainsKey(first))
                        {
                            List<Way> ways = new() { way, graph.FirstNodes[first] };
                            graph.DuplicateFirstNodes.Add(first, ways);
                            foundDuplicateFirst = true;
                        }

                        if (graph.LastNodes.ContainsKey(last))
                        {
                            List<Way> ways = new() { way, graph.LastNodes[last] };
                            graph.DuplicateLastNodes.Add(last, ways);
                            foundDuplicateLast = true;
                        }

                        // Remove start or ends if they already have a pair
                        if (graph.FirstNodes.ContainsKey(last))
                        {
                            graph.FirstNodes.Remove(last);
                        }
                        else if (!foundDuplicateLast)
                        {
                            graph.LastNodes.Add(last, way);
                        }

                        if (graph.LastNodes.ContainsKey(first))
                        {
                            graph.LastNodes.Remove(first);
                        }
                        else if(!foundDuplicateFirst)
                        {
                            graph.FirstNodes.Add(first, way);
                        }
                    }
                }
            }

            // Find ways with wrong direction
            issueWays = issueWays.Concat(FindWaysWrongDirection(graph)).ToList();
            
            // Find unconnected nodes in the coastline
            issueNodes = FindUnconnectedNodes(graph);

            return (issueNodes,issueWays);
        }

        List<(Way way, int issueDetails)> FindWaysWrongDirection(Graph graph)
        {
            List<(Way way, int issueDetails)> found = new();
            if (graph.DuplicateFirstNodes.Count > 0)
            {
                // TODO: Add part to check if there are two connected ways with wrong direction
                foreach (var nodeFirst in graph.DuplicateFirstNodes.Keys)
                {
                    foreach (var nodeLast in graph.DuplicateLastNodes.Keys)
                    {
                        var commonWay = graph.DuplicateFirstNodes[nodeFirst].Intersect(graph.DuplicateLastNodes[nodeLast]).FirstOrDefault();
                        if (commonWay != null)
                        {
                            found.Add((commonWay, 2));
                            graph.FirstNodes.Remove(nodeFirst);
                            graph.LastNodes.Remove(nodeLast);
                        }
                    }
                }
            }
            return found;
        }

        List<(long nodeId, int issueDetails)> FindUnconnectedNodes(Graph graph)
        {
            List<(long nodeId, int issueDetails)> unconnectedNodes = new();
            if(graph.FirstNodes.Count > 0)
            {
                foreach (var node in graph.FirstNodes)
                {
                    unconnectedNodes.Add((node.Key, 1));
                }
            }

            if (graph.LastNodes.Count > 0)
            {
                foreach (var node in graph.LastNodes)
                {
                    unconnectedNodes.Add((node.Key, 1));
                }
            }

            return unconnectedNodes;
        }
    }
}
