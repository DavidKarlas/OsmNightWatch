using OsmSharp.Db;

namespace OsmNightWatch
{
    public interface IOsmGeoBatchSource : IOsmGeoSource
    {
        //TODO: Changes this weird API to GetNodes, GetWays and GetRelations
        void BatchLoad(HashSet<long>? nodeIds = null, HashSet<long>? wayIds = null, HashSet<long>? relationIds = null);
    }
}
