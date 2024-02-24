using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch.Analyzers.HighwayNetwork
{
    public class HighwayNetworkAnalyzer : IOsmAnalyzer
    {
        private KeyValueDatabase database;
        private string dataStoragePath;

        public HighwayNetworkAnalyzer(KeyValueDatabase database, string dataStoragePath)
        {
            this.database = database;
            this.dataStoragePath = dataStoragePath;
        }

        public string AnalyzerName => nameof(HighwayNetworkAnalyzer);

        public FilterSettings FilterSettings { get; } = new FilterSettings() {
            Filters = new List<ElementFilter>() { new ElementFilter(OsmGeoType.Way, new TagFilter[] { new TagFilter("highway") }) }
        };

        public IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoBatchSource newOsmSource)
        {
            throw new NotImplementedException();

        }

        ConcurrentBag<uint> relevantWays = new();
        ConcurrentBag<(long startNode, long endNode)> startEndNodes = new();
        ConcurrentBag<long>[] relevantNodes = Enumerable.Range(0, 13).Select(i => new ConcurrentBag<long>()).ToArray();
        long nodesCount = 0;
        long waysCount = 0;

        public void ProcessElement(OsmGeo element)
        {
            switch (element.Tags!["highway"])
            {
                case "primary":
                case "primary_link":
                case "secondary":
                case "secondary_link":
                case "tertiary":
                case "tertiary_link":
                case "trunk":
                case "trunk_link":
                case "motorway":
                case "motorway_link":
                case "unclassified":
                default:
                    //relevantWays.Add((uint)element.Id);
                    //Interlocked.Increment(ref waysCount);
                    //Interlocked.Add(ref nodesCount, ((Way)element).Nodes.Length);
                    //foreach (var node in ((Way)element).Nodes)
                    //{
                    //    relevantNodes[node / 1000_000_000].Add(node);
                    //}
                    startEndNodes.Add((((Way)element).Nodes[0], ((Way)element).Nodes[^1]));
                    break;
            }
        }

        public IEnumerable<IssueData> ProcessPbf(IOsmGeoBatchSource newOsmSource)
        {
            var deduped = new HashSet<long>();
            foreach (var pair in startEndNodes)
            {
                deduped.Add(pair.startNode);
                deduped.Add(pair.endNode);
            }

            var nodes=NodesParser.LoadNodes(deduped, )

            //Console.WriteLine(relevantWays.Count);
            Console.WriteLine(nodesCount);
            Console.WriteLine(waysCount);
            long sumOfUniqueNodes = 0;
            Parallel.ForEach(relevantNodes, new ParallelOptions() { MaxDegreeOfParallelism = 4 }, (nodes) => {
                var thisBillionCount = new HashSet<long>(nodes).Count;
                Interlocked.Add(ref sumOfUniqueNodes, thisBillionCount);
            });
            Console.WriteLine(sumOfUniqueNodes);

            //Console.WriteLine(new HashSet<long>(relevantNodes).Count);
            //Console.WriteLine(relevantNodes.Count());

            throw new NotImplementedException();
        }
    }
}
