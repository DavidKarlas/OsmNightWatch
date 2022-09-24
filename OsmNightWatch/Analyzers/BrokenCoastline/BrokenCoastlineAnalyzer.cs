using OsmNightWatch.Lib;
using OsmSharp;
using OsmSharp.Changesets;
using OsmSharp.Db;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        public IEnumerable<IssueData> Initialize(IEnumerable<OsmGeo> relevantThings, IOsmGeoSource oldOsmSource, IOsmGeoSource newOsmSource)
        {
            var (issuesNodes, issuesWays) = new CoastlineValidationTest().Visit(relevantThings, newOsmSource);
            if (issuesWays.Count > 0)
            {
                foreach (var (way, detailsNum) in issuesWays)
                {
                    yield return new IssueData()
                    {
                        IssueType = AnalyzerName,
                        OsmType = "way",
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
                        OsmType = "node",
                        OsmId = nodeId,
                        Details = IssueDetails[detailsNum]
                    };
                }
            }
        }

        public IEnumerable<IssueData> AnalyzeChanges(OsmChange changeset, IOsmGeoSource oldOsmSource, IOsmGeoSource newOsmSource)
        {
            var (issuesNodes, issuesWays) = new CoastlineValidationTest().Visit(changeset.Delete.Concat(changeset.Create).Concat(changeset.Modify).OfType<Way>(), newOsmSource);
            if (issuesWays.Count > 0)
            {
                foreach (var (way, detailsNum) in issuesWays)
                {
                    yield return new IssueData()
                    {
                        IssueType = AnalyzerName,
                        OsmType = "way",
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
                        OsmType = "node",
                        OsmId = nodeId,
                        Details = IssueDetails[detailsNum]
                    };
                }
            }
        }
    }
}
