using OsmNightWatch.Lib;
using OsmSharp;

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

        public FilterSettings FilterSettings { get; } = new FilterSettings()
        {
            Filters = new List<ElementFilter>()
            {
                new ElementFilter(OsmGeoType.Way, new[] { new TagFilter("natural", "coastline") })
            }
        };

        public IEnumerable<IssueData> GetIssues(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource)
        {
            //Uncomment line below when we decide we need Nodes information...
            //Utils.BatchLoad(relevantThings, newOsmSource, true, true);
            var (issuesNodes, issuesWays) = new CoastlineValidationTest().Visit(relevantThings, newOsmSource);
            if (issuesWays.Count > 0)
            {
                foreach (var (way, detailsNum) in issuesWays)
                {
                    yield return new IssueData()
                    {
                        IssueType = AnalyzerName,
                        OsmType = "W",
                        OsmId = way.Id!.Value,
                        Details = IssueDetails[detailsNum]
                    };
                }
            }

            if (issuesNodes.Count > 0)
            {
                foreach (var (nodeId, detailsNum) in issuesNodes)
                {
                    yield return new IssueData()
                    {
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
