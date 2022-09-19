using OsmSharp;
using OsmSharp.Db;

namespace OsmNightWatch.Analyzers
{
    public interface IOsmAnalyzer
    {
        string AnalyzerName { get; }

        IEnumerable<ElementFilter> GetFilters();

        bool AnalyzeRelation(Relation relation, IOsmGeoSource oldOsmSource, IOsmGeoSource newOsmSource);
    }
}
