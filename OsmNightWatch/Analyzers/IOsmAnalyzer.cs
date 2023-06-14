using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers
{
    public interface IOsmAnalyzer
    {
        string AnalyzerName { get; }

        FilterSettings FilterSettings { get; }

        IEnumerable<IssueData> ProcessPbf(IEnumerable<OsmGeo> relevantThings, IOsmGeoBatchSource newOsmSource);

        IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoSource newOsmSource);
    }
}
