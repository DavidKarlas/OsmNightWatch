using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers
{
    public interface IOsmAnalyzer
    {
        string AnalyzerName { get; }

        FilterSettings FilterSettings { get; }

        void ProcessElement(OsmGeo element);

        IEnumerable<IssueData> ProcessPbf(IOsmGeoBatchSource newOsmSource);

        IEnumerable<IssueData> ProcessChangeset(MergedChangeset changeSet, IOsmGeoBatchSource newOsmSource);
    }
}
