using OsmNightWatch;
using OsmSharp;

internal class PbfDatabaseWithProcessedChangesets : IOsmGeoBatchSource
{
    private PbfDatabase pbfDb;

    public PbfDatabaseWithProcessedChangesets(PbfDatabase pbfDb)
    {
        this.pbfDb = pbfDb;
    }

    public void BatchLoad(HashSet<long> nodeIds, HashSet<long> wayIds, HashSet<long> relationIds)
    {
        this.pbfDb.BatchLoad(nodeIds, wayIds, relationIds);
    }

    public OsmGeo Get(OsmGeoType type, long id)
    {
        throw new NotImplementedException();
    }
}