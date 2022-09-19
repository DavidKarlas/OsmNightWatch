using OsmSharp;
using OsmSharp.Changesets;
using OsmSharp.Db;

namespace OsmNightWatch
{
    internal class OsmDatabaseWithChangeset : IOsmGeoSource
    {
        private readonly IOsmGeoSource baseSource;
        private Dictionary<long, Relation> changesetRelations = new Dictionary<long, Relation>();
        private Dictionary<long, Way> changesetWays = new Dictionary<long, Way>();
        private Dictionary<long, Node> changesetNodes = new Dictionary<long, Node>();

        public OsmDatabaseWithChangeset(IOsmGeoSource baseSource, OsmChange changeset)
        {
            this.baseSource = baseSource;
            foreach (var change in changeset.Delete)
            {
                switch (change.Type)
                {
                    case OsmGeoType.Node:
                        if (change.Id is long idNode)
                            changesetNodes[idNode] = null;
                        break;
                    case OsmGeoType.Way:
                        if (change.Id is long idWay)
                            changesetWays[idWay] = null;
                        break;
                    case OsmGeoType.Relation:
                        if (change.Id is long idRelation)
                            changesetRelations[idRelation] = null;
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public OsmGeo Get(OsmGeoType type, long id)
        {
            throw new NotImplementedException();
        }
    }
}
