using OsmSharp;
using OsmSharp.Db;

namespace OsmNightWatch
{
    public interface IOsmAnalyzer
    {
        string AnalyzerName { get; }

        List<(OsmGeoType Type, List<(string TagKey, string TagValue)>)> GetFilter();

        bool AnalyzeRelation(Relation relation, IOsmGeoSource oldOsmSource, IOsmGeoSource newOsmSource);
    }
}
