using OsmSharp;
using OsmSharp.Changesets;
using OsmSharp.Db;

namespace OsmNightWatch
{
    public class OsmDatabaseWithChangeset : IOsmGeoSource
    {
        private readonly IOsmGeoSource baseSource;
        private Dictionary<long, Relation?> changesetRelations = new();
        private Dictionary<long, Way?> changesetWays = new();
        private Dictionary<long, Node?> changesetNodes = new();

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
            foreach (var change in changeset.Modify)
            {
                switch (change.Type)
                {
                    case OsmGeoType.Node:
                        if (change.Id is long idNode)
                            changesetNodes[idNode] = change as Node;
                        break;
                    case OsmGeoType.Way:
                        if (change.Id is long idWay)
                            changesetWays[idWay] = change as Way;
                        break;
                    case OsmGeoType.Relation:
                        if (change.Id is long idRelation)
                            changesetRelations[idRelation] = change as Relation;
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
            foreach (var change in changeset.Create)
            {
                switch (change.Type)
                {
                    case OsmGeoType.Node:
                        if (change.Id is long idNode)
                            changesetNodes[idNode] = change as Node;
                        break;
                    case OsmGeoType.Way:
                        if (change.Id is long idWay)
                            changesetWays[idWay] = change as Way;
                        break;
                    case OsmGeoType.Relation:
                        if (change.Id is long idRelation)
                            changesetRelations[idRelation] = change as Relation;
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public OsmGeo Get(OsmGeoType type, long id)
        {
            switch (type)
            {
                case OsmGeoType.Node:
                    if (changesetNodes.TryGetValue(id, out var node))
                    {
                        return node;
                    }
                    break;
                case OsmGeoType.Way:
                    if (changesetWays.TryGetValue(id, out var way))
                    {
                        return way;
                    }
                    break;
                case OsmGeoType.Relation:
                    if (changesetRelations.TryGetValue(id, out var relation))
                    {
                        return relation;
                    }
                    break;
            }
            return baseSource.Get(type, id);
        }
    }
}
