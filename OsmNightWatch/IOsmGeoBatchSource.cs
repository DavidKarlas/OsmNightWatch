using OsmSharp.Db;

namespace OsmNightWatch
{
    public interface IOsmGeoBatchSource : IOsmGeoSource
    {
        void BatchLoad(HashSet<long> nodeIds, HashSet<long> wayIds, HashSet<long> relationIds);
    }
}
