using OsmNightWatch.Lib;
using OsmSharp;
using OsmSharp.Changesets;
using OsmSharp.Db;

namespace OsmNightWatch.Analyzers
{
    public interface IOsmAnalyzer
    {
        string AnalyzerName { get; }

        FilterSettings FilterSettings { get; }

        Func<OsmGeo, IssueData?> GetValidator();
    }
}
