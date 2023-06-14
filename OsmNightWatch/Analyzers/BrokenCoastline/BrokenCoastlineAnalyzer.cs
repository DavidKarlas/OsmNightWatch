using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers.BrokenCoastline
{
    public class BrokenCoastlineAnalyzer : IOsmAnalyzer
    {
        public string AnalyzerName => "BrokenCoastLine";

        public Dictionary<int, string> IssueDetails = new()
        {
            { 1, "Unconnected node in the coastline" },
            { 2, "Way with wrong direction" },
            { 3, "Way not forming a proper ring" }
        };
        private KeyValueDatabase database;
        private Dictionary<long, Way>? existingRelevantThings;

        public BrokenCoastlineAnalyzer(KeyValueDatabase database)
        {
            this.database = database;
        }

        public FilterSettings FilterSettings { get; } = new FilterSettings() {
            Filters = new List<ElementFilter>()
            {
                new ElementFilter(OsmGeoType.Way,
                    new[] { new TagFilter("natural", "coastline") },
                    false,
                    false)
            }
        };

        public IEnumerable<IssueData> ProcessPbf(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
        {
            existingRelevantThings = relevantThings.ToDictionary(r => r.Id, r => (Way)r);
            database.StoreCoastline(existingRelevantThings!);
            return ProcessAllWays(existingRelevantThings.Values);
        }

        public IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoSource newOsmSource)
        {
            if (existingRelevantThings == null)
            {
                existingRelevantThings = database.LoadCoastline();
            }
            var waysToUpdate = new Dictionary<long, Way?>();
            foreach (var newOrModifiedWay in changeSet.Ways)
            {
                if (newOrModifiedWay.Value == null)
                {
                    if (existingRelevantThings.Remove(newOrModifiedWay.Key))
                    {
                        waysToUpdate.Add(newOrModifiedWay.Key, null);
                    }
                }
                else if ((newOrModifiedWay.Value.Tags?.TryGetValue("natural", out var naturalValue) ?? false) && naturalValue == "coastline")
                {
                    existingRelevantThings[newOrModifiedWay.Key] = newOrModifiedWay.Value;
                    waysToUpdate[newOrModifiedWay.Key] = newOrModifiedWay.Value;
                }
                else
                {
                    if (existingRelevantThings.Remove(newOrModifiedWay.Key))
                    {
                        waysToUpdate.Add(newOrModifiedWay.Key, null);
                    }
                }
            }
            database.StoreCoastline(waysToUpdate);
            return ProcessAllWays(existingRelevantThings.Values);
        }

        private IEnumerable<IssueData> ProcessAllWays(IEnumerable<Way> relevantThings)
        {
            var (issuesNodes, issuesWays) = new CoastlineValidationTest().Visit(relevantThings);
            if (issuesWays.Count > 0)
            {
                foreach (var (way, detailsNum) in issuesWays)
                {
                    yield return new IssueData() {
                        IssueType = AnalyzerName,
                        OsmType = "W",
                        OsmId = way.Id,
                        Details = IssueDetails[detailsNum]
                    };
                }
            }

            if (issuesNodes.Count > 0)
            {
                foreach (var (nodeId, detailsNum) in issuesNodes)
                {
                    yield return new IssueData() {
                        IssueType = AnalyzerName,
                        OsmType = "N",
                        OsmId = nodeId,
                        Details = IssueDetails[detailsNum]
                    };
                }
            }
        }
    }
}
